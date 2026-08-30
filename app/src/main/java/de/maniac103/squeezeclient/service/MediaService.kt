/*
 * This file is part of Squeeze Client, an Android client for the LMS music server.
 * Copyright (c) 2024 Danny Baumann
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 */

package de.maniac103.squeezeclient.service

import android.app.PendingIntent
import android.content.ContentResolver
import android.content.Context
import android.content.Intent
import android.content.pm.ServiceInfo
import android.graphics.Bitmap
import android.net.Uri
import android.os.Bundle
import android.os.IBinder
import android.os.Looper
import android.util.Log
import androidx.annotation.OptIn
import androidx.car.app.connection.CarConnection
import androidx.core.app.NotificationCompat
import androidx.core.app.ServiceCompat
import androidx.core.content.IntentCompat
import androidx.core.content.edit
import androidx.core.net.toUri
import androidx.core.os.bundleOf
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.ServiceLifecycleDispatcher
import androidx.lifecycle.coroutineScope
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import androidx.media3.common.C
import androidx.media3.common.DeviceInfo
import androidx.media3.common.MediaItem
import androidx.media3.common.MediaMetadata
import androidx.media3.common.Player
import androidx.media3.common.SimpleBasePlayer
import androidx.media3.common.util.BitmapLoader
import androidx.media3.common.util.UnstableApi
import androidx.media3.datasource.DataSourceBitmapLoader
import androidx.media3.datasource.DefaultDataSource
import androidx.media3.datasource.okhttp.OkHttpDataSource
import androidx.media3.session.CacheBitmapLoader
import androidx.media3.session.CommandButton
import androidx.media3.session.DefaultMediaNotificationProvider
import androidx.media3.session.LibraryResult
import androidx.media3.session.MediaLibraryService
import androidx.media3.session.MediaSession
import androidx.media3.session.MediaSession.ConnectionResult
import androidx.media3.session.SessionCommand
import androidx.media3.session.SessionResult
import com.google.common.collect.ImmutableList
import com.google.common.util.concurrent.Futures
import com.google.common.util.concurrent.ListenableFuture
import de.maniac103.squeezeclient.R
import de.maniac103.squeezeclient.cometd.ConnectionHelper
import de.maniac103.squeezeclient.cometd.ConnectionState
import de.maniac103.squeezeclient.cometd.request.PlaybackButtonRequest
import de.maniac103.squeezeclient.extfuncs.androidAutoPresetButtonCount
import de.maniac103.squeezeclient.extfuncs.connectionHelper
import de.maniac103.squeezeclient.extfuncs.defaultPlayer
import de.maniac103.squeezeclient.extfuncs.httpClient
import de.maniac103.squeezeclient.extfuncs.lastSelectedPlayer
import de.maniac103.squeezeclient.extfuncs.localPlayerEnabled
import de.maniac103.squeezeclient.extfuncs.onlyControlDefaultPlayer
import de.maniac103.squeezeclient.extfuncs.prefs
import de.maniac103.squeezeclient.extfuncs.putLastSelectedPlayer
import de.maniac103.squeezeclient.extfuncs.volumeStepSize
import de.maniac103.squeezeclient.service.localplayer.LocalPlaybackService
import de.maniac103.squeezeclient.model.JiveAction
import de.maniac103.squeezeclient.model.PagingParams
import de.maniac103.squeezeclient.model.PlayerId
import de.maniac103.squeezeclient.model.PlayerStatus
import de.maniac103.squeezeclient.model.Playlist
import de.maniac103.squeezeclient.model.SlimBrowseItemList
import de.maniac103.squeezeclient.ui.MainActivity
import java.io.ByteArrayOutputStream
import java.io.File
import kotlin.time.Clock
import kotlin.time.Duration.Companion.minutes
import kotlin.time.DurationUnit
import kotlin.time.ExperimentalTime
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.flatMapLatest
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.mapNotNull
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.onEach
import kotlinx.coroutines.guava.await
import kotlinx.coroutines.guava.future
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import androidx.media3.session.MediaLibraryService.MediaLibrarySession
import androidx.media3.session.SessionError

// Shared cache converting artwork URLs (HTTP) to content:// URIs (via AlbumArtFileProvider)
// plus downscaled JPEG bytes. Used by both MediaService (browse tree / favorites) and
// SqueezeboxPlayer (now playing / playlist) so each image is fetched only once. Android
// Auto/AAOS need a local content:// URI they can resolve via ContentResolver from their own
// process; a plain https URL or embedded bitmap isn't reliably used by those surfaces.
// MediaMetadataCompat artwork (for lockscreen/notification) goes via Binder IPC to the
// receiving process, with a hard ~1MB per-transaction limit, hence the downscale + JPEG compress.
@OptIn(UnstableApi::class)
private class AlbumArtworkCache(
    private val appContext: Context,
    private val bitmapLoader: BitmapLoader,
    private val scope: CoroutineScope,
    private val onArtworkReady: () -> Unit
) {
    private val dataCache = mutableMapOf<String, ByteArray>()
    private val uriCache = mutableMapOf<String, Uri>()
    private val fetchesInProgress = mutableSetOf<String>()

    // Bounds how many artwork images are decoded (as full-size Bitmaps) at once. Without this,
    // loading a large playlist fires one concurrent decode per track, spiking native memory.
    private val decodeSemaphore = Semaphore(4)

    fun dataFor(url: String): ByteArray? = dataCache[url]
    fun uriFor(url: String): Uri? = uriCache[url]

    fun prefetch(url: String) {
        if (uriCache.containsKey(url) || !fetchesInProgress.add(url)) return
        Log.d(TAG, "Artwork prefetch started for $url")
        scope.launch {
            try {
                val bytes = decodeSemaphore.withPermit {
                    val bitmap = bitmapLoader.loadBitmap(url.toUri()).await()
                    val scaled = scaleForEmbedding(bitmap)
                    val result = ByteArrayOutputStream().use { stream ->
                        scaled.compress(Bitmap.CompressFormat.JPEG, 90, stream)
                        stream.toByteArray()
                    }
                    if (scaled !== bitmap) bitmap.recycle()
                    result
                }

                val cacheDir = File(appContext.cacheDir, "album_art_cache").apply { mkdirs() }
                val file = File(cacheDir, "${url.hashCode()}.jpg")
                file.writeBytes(bytes)
                val contentUri = Uri.Builder()
                    .scheme(ContentResolver.SCHEME_CONTENT)
                    .authority("${appContext.packageName}.albumart")
                    .appendPath(file.name)
                    .build()

                dataCache[url] = bytes
                uriCache[url] = contentUri
                Log.d(TAG, "Artwork prefetch succeeded for $url (${bytes.size} bytes) -> $contentUri")
                onArtworkReady()
            } catch (e: Exception) {
                Log.w(TAG, "Artwork prefetch failed for $url", e)
            } finally {
                fetchesInProgress.remove(url)
            }
        }
    }

    private fun scaleForEmbedding(bitmap: Bitmap): Bitmap {
        val largestDimension = maxOf(bitmap.width, bitmap.height)
        if (largestDimension <= MAX_ARTWORK_DIMENSION_PX) return bitmap
        val scale = MAX_ARTWORK_DIMENSION_PX.toFloat() / largestDimension
        val targetWidth = (bitmap.width * scale).toInt().coerceAtLeast(1)
        val targetHeight = (bitmap.height * scale).toInt().coerceAtLeast(1)
        return Bitmap.createScaledBitmap(bitmap, targetWidth, targetHeight, true)
    }

    companion object {
        private const val TAG = "AlbumArtworkCache"
        private const val MAX_ARTWORK_DIMENSION_PX = 320
    }
}

// For now playing/playlist items: provide both the content URI (for AA/AAOS) and the
// embedded JPEG bytes (for lockscreen/notification).
@OptIn(UnstableApi::class)
private fun MediaMetadata.Builder.applyArtworkUriAndData(
    cache: AlbumArtworkCache,
    url: String?
): MediaMetadata.Builder = apply {
    if (url == null) return@apply
    cache.prefetch(url)
    setArtworkUri(cache.uriFor(url) ?: url.toUri())
    cache.dataFor(url)?.let { setArtworkData(it, MediaMetadata.PICTURE_TYPE_FRONT_COVER) }
}

// For browse-tree items (favorites): provide only the content URI. Google's own guideline
// warns against embedded bitmaps for list items (setIconBitmap/setArtworkData) due to the
// binder limit and lack of AAOS support.
@OptIn(UnstableApi::class)
private fun MediaMetadata.Builder.applyArtworkUri(
    cache: AlbumArtworkCache,
    url: String?
): MediaMetadata.Builder = apply {
    if (url == null) return@apply
    cache.prefetch(url)
    setArtworkUri(cache.uriFor(url) ?: url.toUri())
}

@kotlin.OptIn(ExperimentalTime::class)
class MediaService :
    MediaLibraryService(),
    LifecycleOwner,
    MediaLibrarySession.Callback {
    private val dispatcher = ServiceLifecycleDispatcher(this)
    override val lifecycle: Lifecycle get() = dispatcher.lifecycle
    private lateinit var player: SqueezeboxPlayer
    private lateinit var mediaSession: MediaLibrarySession
    private lateinit var customLayout: List<CommandButton>
    private lateinit var artworkCache: AlbumArtworkCache
    private var lastDisconnectionTime = Clock.System.now()
    private var isConnectedToCar = false
    private var favoritesRefreshJob: Job? = null

    // Stable, fetched-once order (mediaIds) of the favorites list. Without this cache, each
    // onGetChildren(NODE_FAVORITES, ...) call (one per page, requested separately by AA) would
    // trigger a new LMS fetch; if fetches aren't returned in identical order, consecutive pages
    // could get data from different snapshots, shuffling the order in AA. The MediaItems
    // themselves are always rebuilt fresh (from favoriteItemCache, no network needed) so newly
    // arrived icons are included immediately.
    private var cachedFavoriteOrder: List<String>? = null
    private var cachedFavoritesPlayerId: PlayerId? = null

    // AA browse tree caches. Media3 only lets us hand out mediaId strings, so we need to
    // remember which SlimBrowseItem / JiveAction a generated id actually refers to whenever
    // the AA host asks us to go deeper (onGetChildren) or execute an item (onAddMediaItems).
    private val favoriteItemCache = mutableMapOf<String, SlimBrowseItemList.SlimBrowseItem>()
    private val favoriteActionCache = mutableMapOf<String, JiveAction>()

    @OptIn(UnstableApi::class)
    override fun onCreate() {
        dispatcher.onServicePreSuperOnCreate()
        super.onCreate()
        val dataSourceFactory = DefaultDataSource.Factory(
            this,
            OkHttpDataSource.Factory(httpClient)
        )
        val cacheBitmapLoader = CacheBitmapLoader(
            DataSourceBitmapLoader.Builder(this)
                .setDataSourceFactory(dataSourceFactory)
                .build()
        )
        artworkCache = AlbumArtworkCache(applicationContext, cacheBitmapLoader, lifecycleScope) {
            // Could be now-playing artwork or a favorite; refreshing both sides is cheap and
            // avoids tracking which one it was. notifyChildrenChanged is debounced since icons
            // for e.g. 13 favorites arrive close together and shouldn't trigger 13 refreshes.
            player.notifyArtworkUpdated()
            favoritesRefreshJob?.cancel()
            favoritesRefreshJob = lifecycleScope.launch {
                delay(300)
                mediaSession.notifyChildrenChanged(NODE_FAVORITES, Int.MAX_VALUE, null)
            }
        }
        player = SqueezeboxPlayer(applicationContext, connectionHelper, lifecycle, artworkCache)

        val channelInfo = NotificationIds.CHANNEL_MEDIA_CONTROL
        setMediaNotificationProvider(
            DefaultMediaNotificationProvider.Builder(this)
                .setNotificationId(NotificationIds.MEDIA_CONTTROL_SERVICE)
                .setChannelId(channelInfo.id)
                .setChannelName(channelInfo.nameResId)
                .build()
        )

        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                connectionHelper.state.collectLatest { status ->
                    when (status) {
                        is ConnectionState.Disconnected -> handleDisconnection()
                        is ConnectionState.Connecting -> {}
                        is ConnectionState.Connected -> handleConnection(status)
                        is ConnectionState.ConnectionFailure -> handleDisconnection()
                    }
                }
            }
        }

        // When Squeeze Client's own local player starts playing, automatically switch to it as
        // the controlled player. Otherwise AA (and the rest of the UI) keeps trying to control a
        // previously selected, different player while the audio actually comes from the local
        // player - matching the "preferred player takes priority on start" setting in Lyrion's
        // Material skin.
        // Detection uses the model name "squeezeclient" reported only by our own local player
        // (see SlimprotoSocket.sendHello). Deliberately no IP matching and no detection of other
        // local players (e.g. a standalone Squeezelite app): AA claims audio focus for Squeeze
        // Client once that player is active, which simply pauses another app on the same device
        // (a per-app audio focus conflict that can't be resolved from our code).
        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                connectionHelper.state
                    .mapNotNull { (it as? ConnectionState.Connected)?.players }
                    .map { players -> players.filter { it.model == LOCAL_PLAYER_MODEL } }
                    .distinctUntilChanged()
                    .onEach { candidates ->
                        Log.d(
                            TAG,
                            "Local player candidates: " +
                                candidates.joinToString { "${it.id.id} (${it.name})" }
                        )
                        // Also tracked independently of the auto-switch moment: determines
                        // whether getState() reports PLAYBACK_TYPE_LOCAL instead of
                        // PLAYBACK_TYPE_REMOTE (see SqueezeboxPlayer.localPlayerIds), so AA
                        // doesn't show "on another device" when the audio is actually local.
                        player.localPlayerIds = candidates.map { it.id }.toSet()
                    }
                    .flatMapLatest { candidates ->
                        if (candidates.isEmpty()) {
                            emptyFlow()
                        } else {
                            candidates.map { candidate ->
                                connectionHelper.playerState(candidate.id)
                                    .flatMapLatest { it.playStatus }
                                    .map { status -> candidate.id to status.playbackState }
                            }.merge()
                        }
                    }
                    .collectLatest { (candidateId, playbackState) ->
                        // With "Only control default" enabled: never auto-switch, not even to
                        // the local player - the user wants to always control the same,
                        // pre-chosen player without surprises.
                        val restrictedToOtherPlayer = prefs.onlyControlDefaultPlayer &&
                            prefs.defaultPlayer != null &&
                            prefs.defaultPlayer != candidateId
                        if (
                            playbackState == PlayerStatus.PlayState.Playing &&
                            player.currentPlayer != candidateId &&
                            !restrictedToOtherPlayer
                        ) {
                            Log.d(
                                TAG,
                                "Local player $candidateId started playing, " +
                                    "switching to it as active player"
                            )
                            player.currentPlayer = candidateId
                            prefs.edit { putLastSelectedPlayer(candidateId) }
                        }
                    }
            }
        }

        // With "Only control default" always use the chosen default player instead of the
        // last selected one - that's the point of this setting: no switching behavior, always
        // the same player. Without a chosen default player, the last-used player is used.
        player.currentPlayer = prefs.defaultPlayer?.takeIf { prefs.onlyControlDefaultPlayer }
            ?: prefs.lastSelectedPlayer

        // Only show the Mix buttons. Preset buttons (Squeezebox-style, like the physical
        // buttons on a Boom) are appended based on the "aa_preset_button_count" setting (0 = off).
        customLayout = listOf(
            CommandButton.Builder(CommandButton.ICON_UNDEFINED)
                .setDisplayName("Start Mix")
                .setCustomIconResId(R.drawable.ic_sugarcube_mix_24dp)
                .setSessionCommand(SessionCommand(SESSION_ACTION_START_MIX, bundleOf()))
                .build(),
            CommandButton.Builder(CommandButton.ICON_UNDEFINED)
                .setDisplayName("Random Mix")
                .setCustomIconResId(R.drawable.ic_shuffle_song_24dp)
                .setSessionCommand(SessionCommand(SESSION_ACTION_START_RANDOM_MIX, bundleOf()))
                .build()
        ) + presetCommandButtons()

        val activityIntent = PendingIntent.getActivity(
            this,
            0,
            Intent(this, MainActivity::class.java).setAction(Intent.ACTION_MAIN),
            PendingIntent.FLAG_CANCEL_CURRENT or PendingIntent.FLAG_IMMUTABLE
        )

        mediaSession = MediaLibrarySession.Builder(this, player, this)
            .setMediaButtonPreferences(customLayout)
            .setSessionActivity(activityIntent)
            .setBitmapLoader(cacheBitmapLoader)
            .build()
        addSession(mediaSession)

        // Stop playback as soon as the car/head unit disconnects, so music doesn't keep
        // playing unnoticed on the phone itself. CarConnection reliably reports car
        // connectivity (Android Auto via projection or Android Automotive OS native),
        // independent of which MediaSession controllers remain connected.
        CarConnection(this).type.observe(this) { type ->
            val nowConnected = type != CarConnection.CONNECTION_TYPE_NOT_CONNECTED
            Log.d(
                TAG,
                "CarConnection type changed to $type (nowConnected=$nowConnected, " +
                    "wasConnected=$isConnectedToCar, currentPlayer=${player.currentPlayer})"
            )
            if (isConnectedToCar && !nowConnected) {
                val playerId = player.currentPlayer
                if (playerId == null) {
                    Log.w(TAG, "Car disconnected, but no currentPlayer known - no stop sent")
                } else {
                    // See SqueezeboxPlayer.pendingResumePositionMs: the stop command below
                    // resets the position on the server to 0, so save it first.
                    player.pendingResumePositionMs = player.contentPosition
                    Log.d(TAG, "Car disconnected, sending stop command to $playerId")
                    lifecycleScope.launch {
                        try {
                            connectionHelper.changePlaybackState(
                                playerId,
                                PlayerStatus.PlayState.Stopped
                            )
                            Log.d(TAG, "Stop command sent to $playerId")
                        } catch (e: Exception) {
                            Log.w(TAG, "Stop command to $playerId failed", e)
                        }
                    }
                    // The local player doesn't react to server-side stop/power - the
                    // foreground service (ExoPlayer + server connection + notification) would
                    // otherwise keep running while nobody is listening. Confirmed battery
                    // usage (~8% over 28h with barely any use) - hence stopping it explicitly
                    // here instead of just the playback status. Comes back automatically once
                    // the car reconnects (see below).
                    if (playerId in player.localPlayerIds) {
                        Log.d(TAG, "Active player was the local player, stopping LocalPlaybackService")
                        stopService(Intent(this, LocalPlaybackService::class.java))
                    }
                }
            } else if (!isConnectedToCar && nowConnected && prefs.localPlayerEnabled) {
                // Car just connected: the local player may have been stopped above - make
                // sure it's available again in case it's needed.
                Log.d(TAG, "Car connected, restarting LocalPlaybackService if needed")
                LocalPlaybackService.triggerStartOrStop(this)
            }
            isConnectedToCar = nowConnected
        }
    }

    override fun onBind(intent: Intent?): IBinder? {
        dispatcher.onServicePreSuperOnBind()
        return super.onBind(intent)
    }

    override fun onStartCommand(intent: Intent?, flags: Int, startId: Int): Int {
        dispatcher.onServicePreSuperOnStart()
        if (intent?.action == ACTION_START_WITH_PLAYER) {
            // start() (companion object) calls startForegroundService(), which requires
            // Service.startForeground() to be called within a few seconds - otherwise an ANR
            // follows that kills the whole service (and with it the AA session).
            // Media3's own notification logic only promotes to foreground once the player
            // actually becomes "playing"; if that transition takes too long (e.g. when
            // forcing the local player while another player is still stuck), it arrives too
            // late. So set a minimal placeholder notification here immediately; Media3
            // replaces the content once the real playback status is known.
            promoteToForegroundImmediately()
            val playerId = requireNotNull(
                IntentCompat.getParcelableExtra(intent, "playerId", PlayerId::class.java)
            )
            player.currentPlayer = playerId
            return START_STICKY
        }
        return super.onStartCommand(intent, flags, startId)
    }

    private fun promoteToForegroundImmediately() {
        val channelInfo = NotificationIds.CHANNEL_MEDIA_CONTROL
        val notification = NotificationCompat.Builder(this, channelInfo.id)
            .setSmallIcon(R.drawable.ic_logo_notification_24dp)
            .setContentTitle(getString(R.string.app_name))
            .build()
        try {
            ServiceCompat.startForeground(
                this,
                NotificationIds.MEDIA_CONTTROL_SERVICE,
                notification,
                ServiceInfo.FOREGROUND_SERVICE_TYPE_MEDIA_PLAYBACK
            )
        } catch (e: Exception) {
            Log.w(TAG, "Immediate foreground promotion failed", e)
        }
    }

    @OptIn(UnstableApi::class)
    override fun onTaskRemoved(rootIntent: Intent?) {
        if (player.playbackState != Player.STATE_READY && !player.playWhenReady) {
            stopSelf()
        }
    }

    @OptIn(UnstableApi::class)
    override fun onDestroy() {
        dispatcher.onServicePreSuperOnDestroy()
        player.release()
        mediaSession.release()
        super.onDestroy()
    }

    override fun onGetSession(controllerInfo: MediaSession.ControllerInfo) = mediaSession

    @OptIn(UnstableApi::class)
    override fun onConnect(
        session: MediaSession,
        controller: MediaSession.ControllerInfo
    ): ConnectionResult {
        val sessionCommandsBuilder = ConnectionResult.DEFAULT_SESSION_AND_LIBRARY_COMMANDS
            .buildUpon()
            .add(SessionCommand(SESSION_ACTION_POWER, Bundle.EMPTY))
            .add(SessionCommand(SESSION_ACTION_DISCONNECT, Bundle.EMPTY))
            .add(SessionCommand(SESSION_ACTION_START_MIX, Bundle.EMPTY))
            .add(SessionCommand(SESSION_ACTION_START_RANDOM_MIX, Bundle.EMPTY))
        for (slot in 1..prefs.androidAutoPresetButtonCount) {
            sessionCommandsBuilder.add(SessionCommand(presetSessionAction(slot), Bundle.EMPTY))
        }
        val sessionCommands = sessionCommandsBuilder.build()
        return ConnectionResult.AcceptedResultBuilder(session)
            .setAvailableSessionCommands(sessionCommands)
            .setMediaButtonPreferences(customLayout)
            .build()
    }

    // Playback resumption (Media3/AA): without this override the user must always pick
    // something themselves at startup (e.g. a favorite) before anything plays - AA has no
    // idea what "resume" should mean. Deliberately return only one media item here (the
    // current track): once the real, live server status arrives, getState() overwrites this
    // with the full, current playlist anyway. Actually starting playback after this call
    // goes through handleSetPlayWhenReady, which sends a real play command to the server for
    // that player.
    @OptIn(UnstableApi::class)
    override fun onPlaybackResumption(
        mediaSession: MediaSession,
        controller: MediaSession.ControllerInfo
    ): ListenableFuture<MediaSession.MediaItemsWithStartPosition> = lifecycleScope.future {
        Log.d(TAG, "onPlaybackResumption called by ${controller.packageName}")
        val playerId = prefs.defaultPlayer?.takeIf { prefs.onlyControlDefaultPlayer }
            ?: prefs.lastSelectedPlayer
            ?: player.currentPlayer
            ?: run {
                Log.w(TAG, "onPlaybackResumption: no player known to resume")
                throw UnsupportedOperationException("No player known to resume")
            }
        Log.d(TAG, "onPlaybackResumption: resuming with player $playerId")
        if (player.currentPlayer != playerId) {
            player.currentPlayer = playerId
        }
        val resumption = withTimeoutOrNull(RESUMPTION_STATUS_TIMEOUT_MS) {
            var result = player.resumptionMediaItemsWithStartPosition()
            while (result == null) {
                delay(200)
                result = player.resumptionMediaItemsWithStartPosition()
            }
            result
        }
        if (resumption == null) {
            Log.w(TAG, "onPlaybackResumption: timeout, no live status received within ${RESUMPTION_STATUS_TIMEOUT_MS}ms")
            throw UnsupportedOperationException("No active status to resume")
        }
        Log.d(TAG, "onPlaybackResumption: resumption succeeded, startPositionMs=${resumption.startPositionMs}")
        resumption
    }

    override fun onCustomCommand(
        session: MediaSession,
        controller: MediaSession.ControllerInfo,
        customCommand: SessionCommand,
        args: Bundle
    ): ListenableFuture<SessionResult> = lifecycleScope.future {
        val result = when (customCommand.customAction) {
            SESSION_ACTION_DISCONNECT -> {
                stopSelf()
                SessionResult.RESULT_SUCCESS
            }

            SESSION_ACTION_POWER -> {
                player.currentPlayer?.let { connectionHelper.togglePower(it) }
                SessionResult.RESULT_SUCCESS
            }

            SESSION_ACTION_START_MIX -> {
                startSugarCubeMix()
                SessionResult.RESULT_SUCCESS
            }

            SESSION_ACTION_START_RANDOM_MIX -> {
                startRandomTrackMix()
                SessionResult.RESULT_SUCCESS
            }

            else -> {
                val slot = presetSlotFromSessionAction(customCommand.customAction)
                if (slot != null) {
                    startPreset(slot)
                    SessionResult.RESULT_SUCCESS
                } else {
                    SessionResult.RESULT_ERROR_NOT_SUPPORTED
                }
            }
        }
        SessionResult(result)
    }

    // ---- Media3 browse tree (used by Android Auto) ----

    @OptIn(UnstableApi::class)
    override fun onGetLibraryRoot(
        session: MediaLibrarySession,
        browser: MediaSession.ControllerInfo,
        params: LibraryParams?
    ): ListenableFuture<LibraryResult<MediaItem>> {
        // AA requests this for the "For you" panel on the dashboard. If we don't supply our
        // own root here, AA falls back to its own logic (picking something from the top of
        // the browse tree, which feels random). Providing a fixed, predictable selection
        // (same order as the Favorites list) avoids that.
        if (params?.isSuggested == true) {
            val suggestedParams = LibraryParams.Builder().setSuggested(true).build()
            return Futures.immediateFuture(LibraryResult.ofItem(suggestedRootItem(), suggestedParams))
        }
        val rootItem = MediaItem.Builder()
            .setMediaId(NODE_ROOT)
            .setMediaMetadata(
                MediaMetadata.Builder()
                    .setIsBrowsable(true)
                    .setIsPlayable(false)
                    .setTitle(getString(R.string.app_name))
                    .build()
            )
            .build()
        return Futures.immediateFuture(LibraryResult.ofItem(rootItem, params))
    }

    @OptIn(UnstableApi::class)
    override fun onGetChildren(
        session: MediaLibrarySession,
        browser: MediaSession.ControllerInfo,
        parentId: String,
        page: Int,
        pageSize: Int,
        params: LibraryParams?
    ): ListenableFuture<LibraryResult<ImmutableList<MediaItem>>> = lifecycleScope.future {
        val playerId = player.currentPlayer
            ?: return@future LibraryResult.ofItemList(ImmutableList.of(), params)

        val items = when {
            parentId == NODE_ROOT -> listOf(favoritesRootItem())
            parentId == NODE_SUGGESTED -> fetchSuggestedChildren(playerId)
            parentId == NODE_FAVORITES -> fetchFavoriteChildren(playerId)
            parentId.startsWith(FAVORITE_PREFIX) -> fetchFavoriteActionChildren(playerId, parentId)
            else -> emptyList()
        }
        // page/pageSize must be respected: if we always return the full list, Android Auto
        // ignores our order and assembles its own (incorrect) sequence of "pages" from what
        // is actually the same full list every time.
        val startIndex = (page.toLong() * pageSize).coerceIn(0, items.size.toLong()).toInt()
        val endIndex = (startIndex.toLong() + pageSize).coerceIn(startIndex.toLong(), items.size.toLong())
            .toInt()
        val pagedItems = items.subList(startIndex, endIndex)
        LibraryResult.ofItemList(ImmutableList.copyOf(pagedItems), params)
    }

    @OptIn(UnstableApi::class)
    override fun onGetItem(
        session: MediaLibrarySession,
        browser: MediaSession.ControllerInfo,
        mediaId: String
    ): ListenableFuture<LibraryResult<MediaItem>> = lifecycleScope.future {
        val item = when {
            mediaId == NODE_SUGGESTED -> suggestedRootItem()
            mediaId == NODE_FAVORITES -> favoritesRootItem()
            mediaId.startsWith(FAVORITE_PREFIX) ->
                favoriteItemCache[mediaId]?.toBrowsableMediaItem(mediaId)
            else -> favoriteActionCache[mediaId]?.let {
                MediaItem.Builder()
                    .setMediaId(mediaId)
                    .setMediaMetadata(
                        MediaMetadata.Builder()
                            .setIsBrowsable(false)
                            .setIsPlayable(true)
                            .build()
                    )
                    .build()
            }
        }
        item?.let { LibraryResult.ofItem(it, null) }
            ?: LibraryResult.ofError(SessionError.ERROR_BAD_VALUE)
    }

    private fun favoritesRootItem(): MediaItem = MediaItem.Builder()
        .setMediaId(NODE_FAVORITES)
        .setMediaMetadata(
            MediaMetadata.Builder()
                .setIsBrowsable(true)
                .setIsPlayable(false)
                .setTitle("Favorites")
                .build()
        )
        .build()

    private fun suggestedRootItem(): MediaItem = MediaItem.Builder()
        .setMediaId(NODE_SUGGESTED)
        .setMediaMetadata(
            MediaMetadata.Builder()
                .setIsBrowsable(true)
                .setIsPlayable(false)
                .setTitle("For You")
                .build()
        )
        .build()

    // Same stable order as the Favorites list (so no separate fetch/cache needed) - the
    // first MAX_SUGGESTED_ITEMS of it, so AA's "For You" panel shows something predictable
    // instead of browsing the browse tree randomly on its own.
    private suspend fun fetchSuggestedChildren(playerId: PlayerId): List<MediaItem> =
        fetchFavoriteChildren(playerId).take(MAX_SUGGESTED_ITEMS)

    private suspend fun fetchFavoriteChildren(playerId: PlayerId): List<MediaItem> {
        val orderedIds = cachedFavoriteOrder?.takeIf { cachedFavoritesPlayerId == playerId }
            ?: run {
                val homeMenu = connectionHelper.fetchHomeMenu(playerId)
                // TODO verify: assumes the Favorites item has id "favorites". If not, adjust
                // the key below (or let the title fallback handle it).
                val favoritesEntry = homeMenu["favorites"] ?: homeMenu.values.firstOrNull {
                    it.title.contains("favoriet", ignoreCase = true) ||
                            it.title.contains("favorite", ignoreCase = true)
                }
                val goAction = favoritesEntry?.goAction ?: return emptyList()

                val result = connectionHelper.fetchItemsForAction(
                    playerId,
                    goAction,
                    PagingParams.All,
                    false
                )

                // The server returns favorites in their own (manual/storage) order, not
                // alphabetically - Lyrion's Material web skin sorts them alphabetically
                // itself before displaying. Do the same here, so AA (and the "For You"
                // panel, which reuses this list) matches what's shown in Material instead of
                // the raw, seemingly random server state.
                val sortedItems = result.items.sortedBy { it.title.lowercase() }

                favoriteItemCache.clear()
                val ids = sortedItems.map { item ->
                    val mediaId = "$FAVORITE_PREFIX${item.listPosition}"
                    favoriteItemCache[mediaId] = item
                    mediaId
                }
                cachedFavoriteOrder = ids
                cachedFavoritesPlayerId = playerId
                ids
            }
        return orderedIds.mapNotNull { mediaId ->
            favoriteItemCache[mediaId]?.toBrowsableMediaItem(mediaId)
        }
    }

    private fun SlimBrowseItemList.SlimBrowseItem.toBrowsableMediaItem(
        mediaId: String
    ): MediaItem {
        val metadata = MediaMetadata.Builder()
            .setTitle(title)
            .setSubtitle(subText)
            .setIsBrowsable(true)
            .setIsPlayable(true)
            .applyArtworkUri(artworkCache, extractIconUrl(applicationContext))
            .build()
        return MediaItem.Builder()
            .setMediaId(mediaId)
            .setMediaMetadata(metadata)
            .build()
    }
    private fun resolveDirectPlayAction(mediaId: String): JiveAction? =
        favoriteActionCache[mediaId]
            ?: favoriteItemCache[mediaId]?.actions?.let { it.playAction ?: it.goAction }

    private suspend fun fetchFavoriteActionChildren(
        playerId: PlayerId,
        parentId: String
    ): List<MediaItem> {
        val favoriteItem = favoriteItemCache[parentId] ?: return emptyList()
        val actions = favoriteItem.actions ?: return emptyList()
        val children = mutableListOf<MediaItem>()

        // "Play": the normal go/play action of this favorite itself
        (actions.playAction ?: actions.goAction)?.let { playAction ->
            val actionId = "${parentId}_play"
            favoriteActionCache[actionId] = playAction
            children += MediaItem.Builder()
                .setMediaId(actionId)
                .setMediaMetadata(
                    MediaMetadata.Builder()
                        .setTitle("Play")
                        .setIsBrowsable(false)
                        .setIsPlayable(true)
                        .build()
                )
                .build()
        }

        // Context menu (e.g. SugarCube "start mix"), same mechanism as the info button on
        // the now-playing screen.
        actions.moreAction?.let { moreAction ->
            val contextItems = connectionHelper.fetchItemsForAction(
                playerId,
                moreAction,
                PagingParams.All,
                false
            )
            contextItems.items.forEach { contextItem ->
                val contextAction = contextItem.actions?.goAction
                    ?: contextItem.actions?.doAction
                    ?: contextItem.actions?.playAction
                if (contextAction != null) {
                    val actionId = "${parentId}_ctx${contextItem.listPosition}"
                    favoriteActionCache[actionId] = contextAction
                    children += MediaItem.Builder()
                        .setMediaId(actionId)
                        .setMediaMetadata(
                            MediaMetadata.Builder()
                                .setTitle(contextItem.title)
                                .setIsBrowsable(false)
                                .setIsPlayable(true)
                                .build()
                        )
                        .build()
                }
            }
        }

        return children
    }

    // Called when AA actually selects an item (e.g. "Start Mix"). We intercept our own
    // action items and send them to the server instead of treating them as a normal
    // playable item.
    @OptIn(UnstableApi::class)
    override fun onAddMediaItems(
        session: MediaSession,
        controller: MediaSession.ControllerInfo,
        mediaItems: MutableList<MediaItem>
    ): ListenableFuture<MutableList<MediaItem>> = lifecycleScope.future {
        val remaining = mutableListOf<MediaItem>()
        val playerId = player.currentPlayer
        for (mediaItem in mediaItems) {
            val action = resolveDirectPlayAction(mediaItem.mediaId)
            if (action != null && playerId != null) {
                connectionHelper.fetchItemsForAction(playerId, action, PagingParams.All, false)
            } else {
                remaining += mediaItem
            }
        }
        remaining
    }

    // Called when AA does a "Play" tap (playFromMediaId). Media3 translates that legacy
    // call to onSetMediaItems, not onAddMediaItems.
    @OptIn(UnstableApi::class)
    override fun onSetMediaItems(
        session: MediaSession,
        controller: MediaSession.ControllerInfo,
        mediaItems: MutableList<MediaItem>,
        startIndex: Int,
        startPositionMs: Long
    ): ListenableFuture<MediaSession.MediaItemsWithStartPosition> = lifecycleScope.future {
        val playerId = player.currentPlayer
        Log.d(
            TAG,
            "onSetMediaItems: mediaIds=${mediaItems.map { it.mediaId }} playerId=$playerId"
        )
        for (mediaItem in mediaItems) {
            val action = resolveDirectPlayAction(mediaItem.mediaId)
            Log.d(
                TAG,
                "onSetMediaItems: mediaId=${mediaItem.mediaId} action=$action " +
                    "(cacheHit=${favoriteActionCache.containsKey(mediaItem.mediaId) ||
                        favoriteItemCache.containsKey(mediaItem.mediaId)})"
            )
            if (action != null && playerId != null) {
                try {
                    connectionHelper.fetchItemsForAction(playerId, action, PagingParams.All, false)
                    Log.d(TAG, "onSetMediaItems: action for ${mediaItem.mediaId} sent")
                } catch (e: Exception) {
                    Log.w(TAG, "onSetMediaItems: action for ${mediaItem.mediaId} failed", e)
                }
            } else {
                Log.w(
                    TAG,
                    "onSetMediaItems: no action/player for ${mediaItem.mediaId} " +
                        "(action=$action, playerId=$playerId) - item passed through unchanged"
                )
            }
        }
        MediaSession.MediaItemsWithStartPosition(mediaItems, startIndex, startPositionMs)
    }

    // Called via the "Start Mix" button on the AA Now Playing screen. Fetches the context
    // menu of the current track (same source as the info button in NowPlayingFragment.kt)
    // and directly executes the SugarCube "mix from here" action.
    private suspend fun startSugarCubeMix() {
        val playerId = player.currentPlayer ?: return
        val moreAction = player.currentSongActions?.moreAction ?: return
        val items = connectionHelper.fetchItemsForAction(playerId, moreAction, PagingParams.All, false)
        val mixAction = items.items.firstNotNullOfOrNull { item ->
            val action = item.actions?.doAction ?: item.actions?.goAction
            action?.takeIf { it.cmd.getOrNull(2)?.startsWith("mixfromhere") == true }
        } ?: return
        connectionHelper.fetchItemsForAction(playerId, mixAction, PagingParams.All, false)
    }

    // Called via the "Random Mix" button on the AA Now Playing screen. This is Lyrion's own,
    // built-in Random Mix plugin (separate from SugarCube) - the most basic way to play
    // randomly through the whole library. Same approach as startSugarCubeMix(): look up the
    // homemenu item first, then find the "Track Mix" option in the submenu it opens (cmd
    // ["randomplay","tracks"]) and execute it directly.
    private suspend fun startRandomTrackMix() {
        val playerId = player.currentPlayer ?: return
        val homeMenu = connectionHelper.fetchHomeMenu(playerId)
        // "randomplay" itself is purely a category header with no action of its own. The
        // actual mix types (Track Mix, Album Mix, Artist Mix, ...) are separate sibling
        // items in the same homemenu. "randomtracks" (Track Mix) is the simple "play
        // randomly through your whole library" variant intended here.
        val trackMixEntry = homeMenu["randomtracks"] ?: homeMenu.values.firstOrNull {
            it.title.contains("nummermix", ignoreCase = true) ||
                it.title.contains("track mix", ignoreCase = true)
        }
        val action = trackMixEntry?.goAction ?: trackMixEntry?.doAction ?: return
        connectionHelper.fetchItemsForAction(playerId, action, PagingParams.All, false)
    }

    // Builds the row of preset buttons (Squeezebox-style: preset_1.single through
    // preset_10.single) based on the "aa_preset_button_count" setting (0 = no buttons,
    // feature off). AA shows these buttons (once they no longer fit in the main bar) in an
    // overflow menu where only the icon is visible, not the displayName text - hence a
    // separately numbered icon per slot instead of one shared preset icon.
    private fun presetCommandButtons(): List<CommandButton> =
        (1..prefs.androidAutoPresetButtonCount).map { slot ->
            CommandButton.Builder(CommandButton.ICON_UNDEFINED)
                .setDisplayName("Preset $slot")
                .setCustomIconResId(presetIconResId(slot))
                .setSessionCommand(SessionCommand(presetSessionAction(slot), bundleOf()))
                .build()
        }

    private fun presetIconResId(slot: Int) = when (slot) {
        1 -> R.drawable.ic_preset_1_24dp
        2 -> R.drawable.ic_preset_2_24dp
        3 -> R.drawable.ic_preset_3_24dp
        4 -> R.drawable.ic_preset_4_24dp
        5 -> R.drawable.ic_preset_5_24dp
        6 -> R.drawable.ic_preset_6_24dp
        7 -> R.drawable.ic_preset_7_24dp
        8 -> R.drawable.ic_preset_8_24dp
        9 -> R.drawable.ic_preset_9_24dp
        else -> R.drawable.ic_preset_10_24dp
    }

    private fun presetSessionAction(slot: Int) = "$SESSION_ACTION_PRESET_PREFIX$slot"

    private fun presetSlotFromSessionAction(action: String): Int? {
        if (!action.startsWith(SESSION_ACTION_PRESET_PREFIX)) return null
        return action.removePrefix(SESSION_ACTION_PRESET_PREFIX).toIntOrNull()
    }

    // Called via a "Preset N" button on the AA Now Playing screen. This sends exactly the
    // same CLI command as pressing a physical preset button on e.g. a Boom
    // ("<playerid> button preset_N.single"), so LMS itself determines which favorite/radio
    // station is assigned to that button (configurable via Settings > Player > Presets in
    // the LMS web interface).
    private suspend fun startPreset(slot: Int) {
        val playerId = player.currentPlayer ?: return
        connectionHelper.sendButtonRequest(PlaybackButtonRequest.Preset(playerId, slot))
    }

    private fun handleConnection(status: ConnectionState.Connected) {
        // Update connection status first, because currentPlayer checked below
        // is updated on status changes
        player.isConnectedToServer = true
        player.currentPlayer?.let { playerId ->
            if (status.players.none { it.id == playerId }) {
                // current player is gone
                stopSelf()
            }
        }
    }

    private fun handleDisconnection() {
        if (player.isConnectedToServer) {
            lastDisconnectionTime = Clock.System.now()
            player.isConnectedToServer = false
        }
        // Try reconnecting for some amount of time to handle short interruptions. If we can't
        // connect for longer amounts of time (using 15 minutes as an arbitrarily chosen timeout),
        // stop retrying and shut down the service (which in turn removes the media notification)
        val retryTime = Clock.System.now() - lastDisconnectionTime
        if (retryTime < 15.minutes) {
            connectionHelper.connect()
        } else {
            stopSelf()
        }
    }

    companion object {
        private const val TAG = "MediaService"
        private const val LOCAL_PLAYER_MODEL = "squeezeclient"
        private val ACTION_START_WITH_PLAYER = MediaService::class.java.name + ".startWithPlayer"

        private const val SESSION_ACTION_POWER = "power"
        private const val SESSION_ACTION_DISCONNECT = "disconnect"
        private const val SESSION_ACTION_START_MIX = "start_mix"
        private const val SESSION_ACTION_START_RANDOM_MIX = "start_random_mix"
        private const val SESSION_ACTION_PRESET_PREFIX = "preset_"

        private const val NODE_ROOT = "root"
        private const val NODE_FAVORITES = "favorites"
        private const val NODE_SUGGESTED = "suggested"
        private const val FAVORITE_PREFIX = "fav_"
        // Google's own guideline for AA recommendations: offer around 10 items.
        private const val MAX_SUGGESTED_ITEMS = 10
        // How long to wait for the first live server status during playback resumption
        // before giving up - the connection/subscription may still be in progress right
        // after startup.
        private const val RESUMPTION_STATUS_TIMEOUT_MS = 5000L
        // How long to wait for the local player to be "connected" to the server again
        // before sending a play command anyway (see handleSetPlayWhenReady below).
        private const val LOCAL_PLAYER_RECONNECT_TIMEOUT_MS = 5000L

        fun start(context: Context, playerId: PlayerId) {
            val intent = Intent(context, MediaService::class.java).apply {
                action = ACTION_START_WITH_PLAYER
                putExtra("playerId", playerId)
            }
            context.startForegroundService(intent)
        }
    }

    @OptIn(UnstableApi::class)
    private class SqueezeboxPlayer(
        private val appContext: Context,
        private val connectionHelper: ConnectionHelper,
        private val lifecycle: Lifecycle,
        private val artworkCache: AlbumArtworkCache
    ) : SimpleBasePlayer(Looper.getMainLooper()),
        CoroutineScope by lifecycle.coroutineScope {
        // Called by AlbumArtworkCache once a prefetch is done, so AA/lockscreen/notification
        // see the updated metadata (with artwork).
        fun notifyArtworkUpdated() = invalidateState()

        // For playback resumption (see MediaService.onPlaybackResumption): the current
        // track + position based on the last known live status, or null if there isn't one
        // yet (e.g. right after startup, before the subscription received anything).
        fun resumptionMediaItemsWithStartPosition(): MediaSession.MediaItemsWithStartPosition? {
            val status = latestStatus ?: return null
            val currentSong = status.playlist.nowPlaying ?: return null
            val mediaItem = currentSong.toMediaItemDataBuilder(0).build().mediaItem
            val startPositionMs =
                status.currentPlayPosition?.toLong(DurationUnit.MILLISECONDS) ?: 0L
            return MediaSession.MediaItemsWithStartPosition(listOf(mediaItem), 0, startPositionMs)
        }

        // Stores the playback position at the moment the car disconnects and we stop the
        // player (see MediaService: CarConnection observer). A "stop" command resets the
        // position on the server to 0, so without this, resuming after reconnect always
        // starts at the beginning of the track instead of where it was interrupted.
        // Consumed (and cleared) in handleSetPlayWhenReady once play is sent again.
        var pendingResumePositionMs: Long? = null

        var currentPlayer: PlayerId? = null
            set(value) {
                if (field != value) {
                    field = value
                    updatePlayer(value)
                }
            }
        var isConnectedToServer: Boolean = true
            set(value) {
                if (field != value) {
                    field = value
                    updatePlayer(currentPlayer)
                    invalidateState()
                }
            }

        // IDs of players running on this device itself (recognized via model name, see
        // MediaService.onCreate). Determines whether getState() reports PLAYBACK_TYPE_LOCAL
        // instead of PLAYBACK_TYPE_REMOTE, so AA/Android don't show an "on another device"
        // label while the audio is actually just playing on this device.
        var localPlayerIds: Set<PlayerId> = emptySet()
            set(value) {
                if (field != value) {
                    field = value
                    invalidateState()
                }
            }
        private var latestStatus: PlayerStatus? = null
        val currentSongActions get() = latestStatus?.playlist?.nowPlaying?.actions
        private var latestPlaylist: Playlist? = null
        private var statusSubscription: Job? = null

        override fun handleSetDeviceVolume(deviceVolume: Int, flags: Int) = future {
            val playerId = currentPlayer ?: return@future
            connectionHelper.setVolume(playerId, deviceVolume)
        }

        override fun handleIncreaseDeviceVolume(flags: Int) = future {
            val playerId = currentPlayer ?: return@future
            val currentVolume = latestStatus?.currentVolume ?: return@future
            val stepSize = appContext.prefs.volumeStepSize
            connectionHelper.setVolume(playerId, currentVolume + stepSize)
        }

        override fun handleDecreaseDeviceVolume(flags: Int) = future {
            val playerId = currentPlayer ?: return@future
            val currentVolume = latestStatus?.currentVolume ?: return@future
            val stepSize = appContext.prefs.volumeStepSize
            connectionHelper.setVolume(playerId, currentVolume - stepSize)
        }

        override fun handleSetDeviceMuted(muted: Boolean, flags: Int) = future {
            val playerId = currentPlayer ?: return@future
            connectionHelper.setMuteState(playerId, muted)
        }

        // Without this override, SimpleBasePlayer throws an IllegalStateException ("Missing
        // implementation to handle COMMAND_SET_MEDIA_ITEM(S)") when Media3 tries to apply
        // the media items returned from onPlaybackResumption via setMediaItems(). That crash
        // silently breaks the resumption flow: the track is shown but no play command
        // follows, so the player stays paused until manually resumed. No action needed here:
        // getState() always pulls the actual playlist and position from the live server
        // status (latestStatus/latestPlaylist), not from what arrives here.
        override fun handleSetMediaItems(
            mediaItems: List<MediaItem>,
            startIndex: Int,
            startPositionMs: Long
        ) = future {}

        override fun handleSetPlayWhenReady(playWhenReady: Boolean) = future {
            val playerId = currentPlayer
            if (playerId == null) {
                Log.w(TAG, "handleSetPlayWhenReady($playWhenReady): no currentPlayer known")
                return@future
            }
            // For a local player that's still (re)connecting to the server (e.g. right after
            // an AA reconnect, where LocalPlaybackService has to restart), a play command can
            // arrive too early: the server still sees the player as disconnected and reacts
            // unpredictably (in practice: skipping to the next track and immediately
            // stopping, instead of resuming the current track). So wait briefly here until
            // the player is reconnected before sending play.
            if (playWhenReady && playerId in localPlayerIds && latestStatus?.connected != true) {
                Log.d(TAG, "handleSetPlayWhenReady($playWhenReady): waiting for $playerId to reconnect")
                val reconnected = withTimeoutOrNull(LOCAL_PLAYER_RECONNECT_TIMEOUT_MS) {
                    while (latestStatus?.connected != true) {
                        delay(100)
                    }
                    true
                }
                if (reconnected == null) {
                    Log.w(
                        TAG,
                        "handleSetPlayWhenReady($playWhenReady): timeout, $playerId still " +
                            "not connected after ${LOCAL_PLAYER_RECONNECT_TIMEOUT_MS}ms - " +
                            "sending command anyway"
                    )
                }
            }
            val newState = when {
                playWhenReady -> PlayerStatus.PlayState.Playing
                else -> PlayerStatus.PlayState.Paused
            }
            Log.d(TAG, "handleSetPlayWhenReady($playWhenReady): command $newState to $playerId")
            try {
                connectionHelper.changePlaybackState(playerId, newState)
                Log.d(TAG, "handleSetPlayWhenReady($playWhenReady): command sent to $playerId")
            } catch (e: Exception) {
                Log.w(TAG, "handleSetPlayWhenReady($playWhenReady): command to $playerId failed", e)
                return@future
            }
            // Restore the saved position (see pendingResumePositionMs) after a successful
            // play command, so resuming after an AA disconnect/reconnect doesn't always
            // start at the beginning of the track.
            if (playWhenReady) {
                pendingResumePositionMs?.let { positionMs ->
                    pendingResumePositionMs = null
                    val positionSeconds = ((positionMs + 500) / 1000).toInt()
                    if (positionSeconds > 0) {
                        try {
                            connectionHelper.updatePlaybackPosition(playerId, positionSeconds)
                            Log.d(
                                TAG,
                                "handleSetPlayWhenReady($playWhenReady): position $playerId " +
                                    "restored to ${positionSeconds}s"
                            )
                        } catch (e: Exception) {
                            Log.w(
                                TAG,
                                "handleSetPlayWhenReady($playWhenReady): position $playerId " +
                                    "restore failed",
                                e
                            )
                        }
                    }
                }
            }
        }

        override fun handleSeek(mediaItemIndex: Int, positionMs: Long, seekCommand: Int) = future {
            val playerId = currentPlayer ?: return@future
            when (seekCommand) {
                COMMAND_SEEK_TO_NEXT_MEDIA_ITEM, COMMAND_SEEK_TO_NEXT ->
                    connectionHelper.sendButtonRequest(PlaybackButtonRequest.NextTrack(playerId))

                COMMAND_SEEK_TO_PREVIOUS_MEDIA_ITEM, COMMAND_SEEK_TO_PREVIOUS ->
                    connectionHelper.sendButtonRequest(
                        PlaybackButtonRequest.PreviousTrack(playerId)
                    )

                COMMAND_SEEK_IN_CURRENT_MEDIA_ITEM -> {
                    val positionSeconds = ((positionMs + 500) / 1000).toInt()
                    connectionHelper.updatePlaybackPosition(playerId, positionSeconds)
                }

                COMMAND_SEEK_TO_MEDIA_ITEM -> {
                    val positionSeconds = ((positionMs + 500) / 1000).toInt()
                    connectionHelper.advanceToPlaylistPosition(playerId, mediaItemIndex)
                    if (positionSeconds > 0) {
                        connectionHelper.updatePlaybackPosition(playerId, positionSeconds)
                    }
                }

                else -> {}
            }
        }

        override fun handleStop(): ListenableFuture<*> = future {
            val playerId = currentPlayer ?: return@future
            connectionHelper.changePlaybackState(playerId, PlayerStatus.PlayState.Stopped)
        }

        override fun getState(): State {
            val status = latestStatus
            val currentSong = status?.playlist?.nowPlaying
                ?: return State.Builder()
                    .setPlaybackState(STATE_IDLE)
                    // Without this, availableCommands stays empty whenever nothing is playing
                    // (empty playlist): Media3 then refuses both play() and playFromMediaId
                    // (doesn't route through to onSetMediaItems) because the player claims to
                    // support no commands at all - AA then shows "Can't load your selection"
                    // and selecting a favorite simply stops working.
                    .setAvailableCommands(
                        Player.Commands.Builder()
                            .add(COMMAND_PLAY_PAUSE)
                            .add(COMMAND_SET_MEDIA_ITEM)
                            .add(COMMAND_CHANGE_MEDIA_ITEMS)
                            .build()
                    )
                    .build()

            val currentSongDurationUs =
                status.currentSongDuration?.toLong(DurationUnit.MICROSECONDS)
            val (playlist, currentIndex) = latestPlaylist?.let { list ->
                val currentPosition = status.playlist.currentPosition - 1
                val mediaList: List<MediaItemData> = list.items.mapIndexed { index, item ->
                    val builder = if (index + list.offset == currentPosition) {
                        // Prefer current song from status over playlist item, because the former
                        // may be more up to date (e.g. in case of radio streams)
                        currentSong.toMediaItemDataBuilder(index).apply {
                            currentSongDurationUs?.let { setDurationUs(it) }
                        }
                    } else {
                        item.toMediaItemDataBuilder(index)
                    }
                    builder.build()
                }
                Pair(mediaList, currentPosition - list.offset)
            } ?: currentSong.let { song ->
                val builder = song.toMediaItemDataBuilder(0)
                currentSongDurationUs?.let { builder.setDurationUs(it) }
                Pair(listOf(builder.build()), 0)
            }

            val commandsBuilder = Player.Commands.Builder().apply {
                add(COMMAND_ADJUST_DEVICE_VOLUME_WITH_FLAGS)
                add(COMMAND_GET_DEVICE_VOLUME)
                add(COMMAND_GET_CURRENT_MEDIA_ITEM)
                add(COMMAND_GET_METADATA)
                add(COMMAND_GET_TIMELINE)
                add(COMMAND_PLAY_PAUSE)
                add(COMMAND_SET_MEDIA_ITEM)
                add(COMMAND_CHANGE_MEDIA_ITEMS)
                if (status.currentSongDuration != null &&
                    status.playbackState != PlayerStatus.PlayState.Stopped
                ) {
                    add(COMMAND_SEEK_IN_CURRENT_MEDIA_ITEM)
                }
                add(COMMAND_SEEK_TO_MEDIA_ITEM)
                add(COMMAND_SEEK_TO_NEXT_MEDIA_ITEM)
                add(COMMAND_SEEK_TO_NEXT)
                add(COMMAND_SEEK_TO_PREVIOUS_MEDIA_ITEM)
                add(COMMAND_SEEK_TO_PREVIOUS)
                add(COMMAND_SET_DEVICE_VOLUME_WITH_FLAGS)
                add(COMMAND_STOP)
            }

            val playWhenReady = status.playbackState == PlayerStatus.PlayState.Playing
            val playbackState = when {
                !isConnectedToServer -> STATE_BUFFERING

                !status.powered -> STATE_IDLE

                else -> when (status.playbackState) {
                    PlayerStatus.PlayState.Playing -> STATE_READY
                    PlayerStatus.PlayState.Paused -> STATE_READY
                    PlayerStatus.PlayState.Stopped -> STATE_IDLE
                }
            }

            val builder = State.Builder()
                .setPlaybackState(playbackState)
                .setAvailableCommands(commandsBuilder.build())
                .setContentPositionMs(
                    status.currentPlayPosition?.toLong(DurationUnit.MILLISECONDS) ?: C.TIME_UNSET
                )
                .setDeviceInfo(
                    DeviceInfo.Builder(
                        if (currentPlayer != null && currentPlayer in localPlayerIds) {
                            DeviceInfo.PLAYBACK_TYPE_LOCAL
                        } else {
                            DeviceInfo.PLAYBACK_TYPE_REMOTE
                        }
                    )
                        .setMinVolume(0)
                        .setMaxVolume(100)
                        .build()
                )
                .setPlayWhenReady(playWhenReady, PLAY_WHEN_READY_CHANGE_REASON_REMOTE)
                .setPlaylist(playlist)
                .setCurrentMediaItemIndex(currentIndex)

            status.currentVolume?.let { builder.setDeviceVolume(it) }
            status.muted?.let { builder.setIsDeviceMuted(it) }

            return builder.build()
        }

        @kotlin.OptIn(ExperimentalCoroutinesApi::class)
        private fun updatePlayer(playerId: PlayerId?) {
            statusSubscription?.cancel()
            if (playerId == null || !isConnectedToServer) {
                return
            }
            statusSubscription = launch {
                lifecycle.repeatOnLifecycle(Lifecycle.State.STARTED) {
                    connectionHelper.playerState(playerId)
                        .flatMapLatest { it.playStatus }
                        .collect { status ->
                            if (status.playlist.lastChange != latestStatus?.playlist?.lastChange) {
                                latestPlaylist = connectionHelper.fetchPlaylist(
                                    playerId,
                                    PagingParams.All
                                )
                            }
                            latestStatus = status
                            invalidateState()
                        }
                }
            }
        }

        private fun Playlist.PlaylistItem.toMediaItemDataBuilder(
            position: Int
        ): MediaItemData.Builder {
            val artworkUrl = extractIconUrl(appContext)
            val metadata = MediaMetadata.Builder()
                .setTitle(title)
                .setArtist(artist)
                .setAlbumTitle(album)
                .applyArtworkUriAndData(artworkCache, artworkUrl)
                .build()
            return MediaItemData.Builder(position)
                .setMediaItem(
                    MediaItem.Builder()
                        .setMediaId(position.toString())
                        .setMediaMetadata(metadata)
                        .build()
                )
                .setMediaMetadata(metadata)
        }
    }
}