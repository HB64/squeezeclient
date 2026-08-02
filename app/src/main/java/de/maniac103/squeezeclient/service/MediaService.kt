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
import kotlinx.coroutines.withTimeoutOrNull
import androidx.media3.session.MediaLibraryService.MediaLibrarySession
import androidx.media3.session.SessionError

// Gedeelde cache die artwork-URL's (HTTP) omzet naar content://-URI's (via AlbumArtFileProvider)
// plus verkleinde JPEG-bytes. Wordt zowel door MediaService (browse tree / favorieten) als door
// SqueezeboxPlayer (now playing / afspeellijst) gebruikt, zodat eenzelfde plaatje maar één keer
// hoeft te worden opgehaald. Android Auto/AAOS vereisen een lokale content://-URI die ze zelf via
// ContentResolver kunnen resolven vanuit hun eigen proces; een kale https-URL of een embedded
// bitmap wordt door die surfaces niet (betrouwbaar) gebruikt. MediaMetadataCompat-artwork (voor
// lockscreen/notificatie) gaat via Binder-IPC naar het ontvangende proces, met een harde limiet
// van ~1MB per transactie; vandaar het verkleinen + JPEG-comprimeren.
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

    fun dataFor(url: String): ByteArray? = dataCache[url]
    fun uriFor(url: String): Uri? = uriCache[url]

    fun prefetch(url: String) {
        if (uriCache.containsKey(url) || !fetchesInProgress.add(url)) return
        Log.d(TAG, "Artwork prefetch gestart voor $url")
        scope.launch {
            try {
                val bitmap = bitmapLoader.loadBitmap(url.toUri()).await()
                val scaled = scaleForEmbedding(bitmap)
                val bytes = ByteArrayOutputStream().use { stream ->
                    scaled.compress(Bitmap.CompressFormat.JPEG, 90, stream)
                    stream.toByteArray()
                }
                if (scaled !== bitmap) scaled.recycle()

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
                Log.d(TAG, "Artwork prefetch gelukt voor $url (${bytes.size} bytes) -> $contentUri")
                onArtworkReady()
            } catch (e: Exception) {
                Log.w(TAG, "Artwork prefetch mislukt voor $url", e)
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

// Voor now playing/afspeellijst-items: zowel de content-URI (voor AA/AAOS) als de embedded
// JPEG-bytes (voor lockscreen/notificatie) meegeven.
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

// Voor browse-tree-items (favorieten): alleen de content-URI meegeven. Google's eigen richtlijn
// waarschuwt expliciet tegen embedded bitmaps voor lijst-items (setIconBitmap/setArtworkData) i.v.m.
// de binder-limiet en omdat het op AAOS niet wordt ondersteund.
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

    // Eén keer opgehaalde, stabiele volgorde (mediaId's) van de favorietenlijst. Zonder deze
    // cache deed elke onGetChildren(NODE_FAVORITES, ...)-aanroep (één per pagina, en AA vraagt
    // per pagina apart op) een nieuwe LMS-fetch; als die fetches niet perfect identiek geordend
    // terugkomen (of elkaar overlappen doordat notifyChildrenChanged tijdens het laden van
    // iconen vuurt), kregen opeenvolgende pagina's data uit verschillende "snapshots" — vandaar
    // de door elkaar gehusselde volgorde in AA. De MediaItems zelf worden wel telkens vers
    // opgebouwd (uit favoriteItemCache, geen netwerk nodig) zodat net binnengekomen iconen
    // direct meegenomen worden.
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
            // Kan voor now-playing-artwork of voor een favoriet zijn; beide kanten simpelweg
            // laten verversen is goedkoop en voorkomt dat we moeten bijhouden welke van de twee
            // het was. notifyChildrenChanged debouncen we, want bij bv. 13 favorieten komen de
            // iconen kort na elkaar binnen en willen we niet 13x achter elkaar verversen.
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

        // Zodra Squeeze Client's eigen lokale speler begint met afspelen, daar automatisch naar
        // overschakelen als bediende speler. Zonder dit blijft AA (en de rest van de UI) een
        // eventueel eerder geselecteerde, andere speler proberen te bedienen, terwijl je
        // feitelijk het geluid van de lokale speler hoort — precies zoals de "voorkeurspeler
        // krijgt voorrang bij starten"-instelling in Lyrion's Material skin.
        // Herkenning gebeurt via de modelnaam "squeezeclient" die alleen onze eigen lokale
        // speler meldt (zie SlimprotoSocket.sendHello). Bewust géén IP-matching en géén
        // detectie van andere lokale spelers (zoals een los geïnstalleerde Squeezelite-app):
        // AA claimt audiofocus voor Squeeze Client zodra die speler actief is, wat een andere
        // app op hetzelfde toestel dan gewoon pauzeert (per-app audiofocus-conflict, niet
        // oplosbaar vanuit onze code).
        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                connectionHelper.state
                    .mapNotNull { (it as? ConnectionState.Connected)?.players }
                    .map { players -> players.filter { it.model == LOCAL_PLAYER_MODEL } }
                    .distinctUntilChanged()
                    .onEach { candidates ->
                        Log.d(
                            TAG,
                            "Lokale spelerskandidaten: " +
                                candidates.joinToString { "${it.id.id} (${it.name})" }
                        )
                        // Ook bijhouden los van het auto-switch-moment: bepaalt of getState()
                        // straks PLAYBACK_TYPE_LOCAL i.p.v. PLAYBACK_TYPE_REMOTE rapporteert
                        // (zie SqueezeboxPlayer.localPlayerIds), zodat AA niet "op een ander
                        // apparaat" toont wanneer het geluid feitelijk gewoon lokaal speelt.
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
                        // Als "Only control default" aan staat: nooit automatisch wisselen,
                        // ook niet naar de lokale speler - de gebruiker wil juist altijd
                        // dezelfde, vooraf gekozen speler bedienen zonder verrassingen.
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
                                "Lokale speler $candidateId is gestart met afspelen, " +
                                    "overschakelen als actieve speler"
                            )
                            player.currentPlayer = candidateId
                            prefs.edit { putLastSelectedPlayer(candidateId) }
                        }
                    }
            }
        }

        // Bij "Only control default" altijd de gekozen standaardspeler gebruiken i.p.v. de
        // laatst geselecteerde - dat is precies het punt van deze instelling: geen wisselend
        // gedrag, altijd dezelfde speler. Zonder gekozen standaardspeler blijft het bestaande
        // gedrag (laatst gebruikte speler) ongewijzigd.
        player.currentPlayer = prefs.defaultPlayer?.takeIf { prefs.onlyControlDefaultPlayer }
            ?: prefs.lastSelectedPlayer

        // Alleen de Mix-knoppen tonen. Power/disconnect stonden er ook nog in (leftover uit
        // eerdere iteraties), maar zijn nooit de bedoeling geweest voor deze knoppenrij.
        // Preset-knoppen (Squeezebox-stijl, zoals de fysieke knoppen op een Boom) worden er
        // achteraan toegevoegd op basis van de "aa_preset_button_count"-instelling (0 = uit).
        customLayout = listOf(
            CommandButton.Builder(CommandButton.ICON_UNDEFINED)
                .setDisplayName("Mix starten")
                .setCustomIconResId(R.drawable.ic_sugarcube_mix_24dp)
                .setSessionCommand(SessionCommand(SESSION_ACTION_START_MIX, bundleOf()))
                .build(),
            CommandButton.Builder(CommandButton.ICON_UNDEFINED)
                .setDisplayName("Willekeurige mix")
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

        // Stop de weergave zodra de auto/head unit loskoppelt, zodat muziek niet ongemerkt
        // doorspeelt op de telefoon zelf. CarConnection meldt betrouwbaar de connectiviteit met
        // een auto (Android Auto via projectie of Android Automotive OS native), onafhankelijk
        // van welke MediaSession-controllers er verder verbonden zijn/blijven.
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
                    Log.w(TAG, "Auto losgekoppeld, maar geen currentPlayer bekend - geen stop verstuurd")
                } else {
                    // Zie SqueezeboxPlayer.pendingResumePositionMs: het stop-commando hieronder
                    // reset de positie op de server naar 0, dus die eerst bewaren.
                    player.pendingResumePositionMs = player.contentPosition
                    Log.d(TAG, "Auto losgekoppeld, stop-commando sturen naar $playerId")
                    lifecycleScope.launch {
                        try {
                            connectionHelper.changePlaybackState(
                                playerId,
                                PlayerStatus.PlayState.Stopped
                            )
                            Log.d(TAG, "Stop-commando naar $playerId verstuurd")
                        } catch (e: Exception) {
                            Log.w(TAG, "Stop-commando naar $playerId mislukt", e)
                        }
                    }
                    // De lokale speler reageert niet op server-side stop/power - de
                    // foreground service (ExoPlayer + serverconnectie + notificatie) blijft
                    // anders gewoon draaien terwijl er niemand meer luistert. Bevestigd
                    // accuverbruik (~8% over 28u bij amper gebruik) - daarom hier expliciet
                    // stoppen i.p.v. alleen de afspeelstatus. Komt vanzelf terug zodra de
                    // auto opnieuw verbindt (zie hieronder).
                    if (playerId in player.localPlayerIds) {
                        Log.d(TAG, "Actieve speler was de lokale speler, LocalPlaybackService stoppen")
                        stopService(Intent(this, LocalPlaybackService::class.java))
                    }
                }
            } else if (!isConnectedToCar && nowConnected && prefs.localPlayerEnabled) {
                // Auto net verbonden: de lokale speler kan hierboven gestopt zijn - zorg dat
                // 'ie weer beschikbaar is voor het geval hij (opnieuw) nodig is.
                Log.d(TAG, "Auto verbonden, LocalPlaybackService weer opstarten indien nodig")
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
            // start() (companion object) roept startForegroundService() aan, wat Android
            // verplicht dat we binnen enkele seconden Service.startForeground() aanroepen -
            // anders volgt een ANR die de hele service (en daarmee de AA-sessie) om zeep helpt.
            // Media3's eigen notificatielogica promoveert pas naar foreground zodra de speler
            // daadwerkelijk "playing" wordt; als die overgang te lang duurt (bv. bij het
            // forceren van de local player terwijl een andere speler nog vastzit), komt die
            // belofte te laat. Daarom hier meteen een minimale placeholder-notificatie zetten;
            // Media3 vervangt de inhoud vanzelf zodra de echte afspeelstatus bekend is.
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
            Log.w(TAG, "Direct promoten naar foreground mislukt", e)
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

    // Playback resumption (Media3/AA): zonder deze override moet de gebruiker bij het
    // opstarten altijd eerst zelf iets kiezen (bv. een favoriet) voordat er iets afspeelt -
    // AA heeft dan geen idee wat "hervatten" zou moeten betekenen. We geven hier bewust maar
    // één mediaitem terug (het huidige nummer): zodra de echte, live serverstatus binnenkomt
    // overschrijft getState() dit toch met de volledige, actuele playlist. Het daadwerkelijk
    // starten van de afspeelknop na deze aanroep loopt via handleSetPlayWhenReady, wat een
    // echt play-commando naar de server stuurt voor de betreffende speler.
    @OptIn(UnstableApi::class)
    override fun onPlaybackResumption(
        mediaSession: MediaSession,
        controller: MediaSession.ControllerInfo
    ): ListenableFuture<MediaSession.MediaItemsWithStartPosition> = lifecycleScope.future {
        Log.d(TAG, "onPlaybackResumption aangeroepen door ${controller.packageName}")
        val playerId = prefs.defaultPlayer?.takeIf { prefs.onlyControlDefaultPlayer }
            ?: prefs.lastSelectedPlayer
            ?: player.currentPlayer
            ?: run {
                Log.w(TAG, "onPlaybackResumption: geen speler bekend om te hervatten")
                throw UnsupportedOperationException("Geen speler bekend om te hervatten")
            }
        Log.d(TAG, "onPlaybackResumption: hervatten met speler $playerId")
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
            Log.w(TAG, "onPlaybackResumption: timeout, geen live status ontvangen binnen ${RESUMPTION_STATUS_TIMEOUT_MS}ms")
            throw UnsupportedOperationException("Geen actieve status om te hervatten")
        }
        Log.d(TAG, "onPlaybackResumption: hervatting geslaagd, startPositionMs=${resumption.startPositionMs}")
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
        // AA vraagt dit op voor het "Voor jou"-paneel op het dashboard. Leveren we hier geen
        // eigen root, dan valt AA terug op zijn eigen logica (iets plukken van de bovenkant
        // van de browse-boom, wat als willekeurig aanvoelt). Door zelf een vaste, voorspelbare
        // selectie te geven (dezelfde volgorde als de Favorieten-lijst) voorkomen we dat.
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
        // page/pageSize moeten gerespecteerd worden: als we altijd de volledige lijst
        // teruggeven, negeert Android Auto onze volgorde en stelt het zelf (verkeerd) een
        // opeenvolging van "pagina's" samen uit wat feitelijk steeds dezelfde volledige lijst is.
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
                .setTitle("Favorieten")
                .build()
        )
        .build()

    private fun suggestedRootItem(): MediaItem = MediaItem.Builder()
        .setMediaId(NODE_SUGGESTED)
        .setMediaMetadata(
            MediaMetadata.Builder()
                .setIsBrowsable(true)
                .setIsPlayable(false)
                .setTitle("Voor jou")
                .build()
        )
        .build()

    // Zelfde, stabiele volgorde als de Favorieten-lijst (dus geen aparte fetch/cache nodig) —
    // de eerste MAX_SUGGESTED_ITEMS daarvan, zodat AA's "Voor jou"-paneel iets voorspelbaars
    // toont in plaats van zelf willekeurig door de browse-boom te bladeren.
    private suspend fun fetchSuggestedChildren(playerId: PlayerId): List<MediaItem> =
        fetchFavoriteChildren(playerId).take(MAX_SUGGESTED_ITEMS)

    private suspend fun fetchFavoriteChildren(playerId: PlayerId): List<MediaItem> {
        val orderedIds = cachedFavoriteOrder?.takeIf { cachedFavoritesPlayerId == playerId }
            ?: run {
                val homeMenu = connectionHelper.fetchHomeMenu(playerId)
                // TODO verify: aanname dat het Favorieten-item als id "favorites" heeft. Zo
                // niet, pas de key hieronder aan (of laat de title-fallback het werk doen).
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

                // De server geeft favorieten terug in hun eigen (handmatige/opslag-)volgorde,
                // niet alfabetisch - Lyrion's Material-webskin sorteert dat zelf alfabetisch
                // voordat het getoond wordt. Hier hetzelfde doen, zodat AA (en het "Voor
                // jou"-paneel, dat deze lijst hergebruikt) overeenkomt met wat je in Material
                // ziet in plaats van de ruwe, ogenschijnlijk willekeurige serverstate.
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

        // "Afspelen": de normale go/play-actie van deze favoriet zelf
        (actions.playAction ?: actions.goAction)?.let { playAction ->
            val actionId = "${parentId}_play"
            favoriteActionCache[actionId] = playAction
            children += MediaItem.Builder()
                .setMediaId(actionId)
                .setMediaMetadata(
                    MediaMetadata.Builder()
                        .setTitle("Afspelen")
                        .setIsBrowsable(false)
                        .setIsPlayable(true)
                        .build()
                )
                .build()
        }

        // Context-menu (bijv. SugarCube "mix starten"), zelfde mechanisme als de
        // info-knop op het now-playing-scherm.
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

    // Wordt aangeroepen zodra AA een item (bijv. "Mix starten") daadwerkelijk selecteert.
    // We onderscheppen onze eigen actie-items en sturen ze naar de server i.p.v. ze als
    // normaal afspeelitem te behandelen.
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

    // Wordt aangeroepen wanneer AA een "Afspelen"-tik doet (playFromMediaId). Media3
    // vertaalt die legacy-aanroep naar onSetMediaItems, niet naar onAddMediaItems.
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
                    Log.d(TAG, "onSetMediaItems: actie voor ${mediaItem.mediaId} verstuurd")
                } catch (e: Exception) {
                    Log.w(TAG, "onSetMediaItems: actie voor ${mediaItem.mediaId} mislukt", e)
                }
            } else {
                Log.w(
                    TAG,
                    "onSetMediaItems: geen actie/speler voor ${mediaItem.mediaId} " +
                        "(action=$action, playerId=$playerId) - item wordt ongewijzigd doorgegeven"
                )
            }
        }
        MediaSession.MediaItemsWithStartPosition(mediaItems, startIndex, startPositionMs)
    }

    // Wordt aangeroepen via de "Mix starten"-knop op het AA Now Playing-scherm. Haalt het
    // contextmenu van het huidige nummer op (zelfde bron als de info-knop in
    // NowPlayingFragment.kt) en voert de SugarCube "mix vanaf hier"-actie direct uit.
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

    // Wordt aangeroepen via de "Willekeurige mix"-knop op het AA Now Playing-scherm. Dit is
    // Lyrion's eigen, ingebouwde Random Mix-plugin (los van SugarCube) — het meest rudimentaire
    // principe om willekeurig door de hele bibliotheek te spelen. Zelfde aanpak als
    // startSugarCubeMix(): eerst het homemenu-item opzoeken, dan in het submenu dat opent de
    // "Track Mix"-optie zoeken (cmd ["randomplay","tracks"]) en die direct uitvoeren.
    private suspend fun startRandomTrackMix() {
        val playerId = player.currentPlayer ?: return
        val homeMenu = connectionHelper.fetchHomeMenu(playerId)
        // "randomplay" zelf is puur een categorie-header zonder eigen actie. De daadwerkelijke
        // mixtypes (Nummermix, Albummix, Artiestenmix, ...) staan als losse sibling-items in
        // hetzelfde homemenu. "randomtracks" (Nummermix) is de simpele "speel willekeurig door
        // je hele bibliotheek"-variant die hier bedoeld is.
        val trackMixEntry = homeMenu["randomtracks"] ?: homeMenu.values.firstOrNull {
            it.title.contains("nummermix", ignoreCase = true) ||
                it.title.contains("track mix", ignoreCase = true)
        }
        val action = trackMixEntry?.goAction ?: trackMixEntry?.doAction ?: return
        connectionHelper.fetchItemsForAction(playerId, action, PagingParams.All, false)
    }

    // Bouwt de rij preset-knoppen (Squeezebox-stijl: preset_1.single t/m preset_10.single) op
    // basis van de "aa_preset_button_count"-instelling (0 = geen knoppen, dus feature uit).
    // AA toont deze knoppen (zodra ze niet meer in de hoofdbalk passen) in een uitklap-menu
    // waarin alleen het icoon zichtbaar is, niet de displayName-tekst - vandaar per slot een
    // apart genummerd icoontje in plaats van één gedeeld preset-icoon.
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

    // Wordt aangeroepen via een "Preset N"-knop op het AA Now Playing-scherm. Dit stuurt exact
    // hetzelfde CLI-commando als het indrukken van een fysieke preset-knop op bv. een Boom
    // ("<playerid> button preset_N.single"), dus LMS regelt zelf welk favoriet/radiozender op
    // die knop staat (in te stellen via Instellingen > Speler > Presets in de LMS-webinterface).
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
        // Google's eigen richtlijn voor AA-recommendations: rond de 10 items aanbieden.
        private const val MAX_SUGGESTED_ITEMS = 10
        // Hoe lang op de eerste live serverstatus wachten bij playback resumption, voordat
        // we het opgeven - de verbinding/subscriptie kan vlak na het opstarten nog onderweg zijn.
        private const val RESUMPTION_STATUS_TIMEOUT_MS = 5000L
        // Hoe lang wachten tot de lokale speler weer "connected" is bij de server voordat een
        // play-commando alsnog verstuurd wordt (zie handleSetPlayWhenReady hieronder).
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
        // Aangeroepen door AlbumArtworkCache zodra een prefetch klaar is, zodat AA/lockscreen/
        // notificatie de bijgewerkte metadata (met artwork) te zien krijgen.
        fun notifyArtworkUpdated() = invalidateState()

        // Voor playback resumption (zie MediaService.onPlaybackResumption): het huidige
        // nummer + positie op basis van de laatst bekende live status, of null als die er
        // nog niet is (bv. vlak na het opstarten, voordat de subscriptie iets binnenkreeg).
        fun resumptionMediaItemsWithStartPosition(): MediaSession.MediaItemsWithStartPosition? {
            val status = latestStatus ?: return null
            val currentSong = status.playlist.nowPlaying ?: return null
            val mediaItem = currentSong.toMediaItemDataBuilder(0).build().mediaItem
            val startPositionMs =
                status.currentPlayPosition?.toLong(DurationUnit.MILLISECONDS) ?: 0L
            return MediaSession.MediaItemsWithStartPosition(listOf(mediaItem), 0, startPositionMs)
        }

        // Bewaart de afspeelpositie op het moment dat de auto loskoppelt en we de speler
        // stoppen (zie MediaService: CarConnection-observer). Een "stop"-commando reset de
        // positie op de server naar 0, dus zonder dit begint hervatten na reconnect altijd
        // weer bij het begin van het nummer i.p.v. waar het onderbroken werd. Wordt in
        // handleSetPlayWhenReady verbruikt (en gewist) zodra er weer play gestuurd wordt.
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

        // IDs van spelers die op dit toestel zelf draaien (herkend via modelnaam, zie
        // MediaService.onCreate). Bepaalt of getState() PLAYBACK_TYPE_LOCAL rapporteert i.p.v.
        // PLAYBACK_TYPE_REMOTE, zodat AA/Android geen "op een ander apparaat"-label tonen
        // terwijl het geluid feitelijk gewoon op dit toestel speelt.
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

        // Zonder deze override gooit SimpleBasePlayer een IllegalStateException
        // ("Missing implementation to handle COMMAND_SET_MEDIA_ITEM(S)") zodra Media3 na
        // onPlaybackResumption de teruggegeven media-items via setMediaItems() probeert toe
        // te passen. Die crash breekt de hervattingsflow stilletjes af: het nummer wordt wel
        // getoond maar er volgt geen play-commando, dus de speler blijft gepauzeerd staan
        // totdat je zelf op hervatten drukt. We hoeven hier zelf niets te doen: getState()
        // haalt de daadwerkelijke afspeellijst en positie altijd uit de live serverstatus
        // (latestStatus/latestPlaylist), niet uit wat hier binnenkomt.
        override fun handleSetMediaItems(
            mediaItems: List<MediaItem>,
            startIndex: Int,
            startPositionMs: Long
        ) = future {}

        override fun handleSetPlayWhenReady(playWhenReady: Boolean) = future {
            val playerId = currentPlayer
            if (playerId == null) {
                Log.w(TAG, "handleSetPlayWhenReady($playWhenReady): geen currentPlayer bekend")
                return@future
            }
            // Bij een lokale speler die nog aan het (her)verbinden is met de server (bv. vlak na
            // een AA-reconnect, waarbij LocalPlaybackService opnieuw moet opstarten) kan een
            // play-commando te vroeg aankomen: de server ziet de speler dan nog als disconnected
            // en reageert daar onvoorspelbaar op (in de praktijk: doorspringen naar het volgende
            // nummer en meteen weer stoppen, in plaats van het huidige nummer te hervatten).
            // Daarom hier kort wachten tot de speler weer verbonden is voordat we play sturen.
            if (playWhenReady && playerId in localPlayerIds && latestStatus?.connected != true) {
                Log.d(TAG, "handleSetPlayWhenReady($playWhenReady): wachten tot $playerId weer verbonden is")
                val reconnected = withTimeoutOrNull(LOCAL_PLAYER_RECONNECT_TIMEOUT_MS) {
                    while (latestStatus?.connected != true) {
                        delay(100)
                    }
                    true
                }
                if (reconnected == null) {
                    Log.w(
                        TAG,
                        "handleSetPlayWhenReady($playWhenReady): timeout, $playerId nog steeds " +
                            "niet verbonden na ${LOCAL_PLAYER_RECONNECT_TIMEOUT_MS}ms - " +
                            "commando toch maar versturen"
                    )
                }
            }
            val newState = when {
                playWhenReady -> PlayerStatus.PlayState.Playing
                else -> PlayerStatus.PlayState.Paused
            }
            Log.d(TAG, "handleSetPlayWhenReady($playWhenReady): commando $newState naar $playerId")
            try {
                connectionHelper.changePlaybackState(playerId, newState)
                Log.d(TAG, "handleSetPlayWhenReady($playWhenReady): commando naar $playerId verstuurd")
            } catch (e: Exception) {
                Log.w(TAG, "handleSetPlayWhenReady($playWhenReady): commando naar $playerId mislukt", e)
                return@future
            }
            // Bewaarde positie (zie pendingResumePositionMs) alsnog terugzetten na een
            // geslaagd play-commando, zodat hervatten na een AA-disconnect/reconnect niet
            // steeds weer bij het begin van het nummer begint.
            if (playWhenReady) {
                pendingResumePositionMs?.let { positionMs ->
                    pendingResumePositionMs = null
                    val positionSeconds = ((positionMs + 500) / 1000).toInt()
                    if (positionSeconds > 0) {
                        try {
                            connectionHelper.updatePlaybackPosition(playerId, positionSeconds)
                            Log.d(
                                TAG,
                                "handleSetPlayWhenReady($playWhenReady): positie $playerId " +
                                    "teruggezet naar ${positionSeconds}s"
                            )
                        } catch (e: Exception) {
                            Log.w(
                                TAG,
                                "handleSetPlayWhenReady($playWhenReady): positie $playerId " +
                                    "terugzetten mislukt",
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
                    // Zonder dit blijft availableCommands leeg zodra er niets speelt (lege
                    // playlist): Media3 weigert dan zowel play() als playFromMediaId (routeert
                    // niet door naar onSetMediaItems) omdat de speler claimt geen enkel
                    // commando te ondersteunen - AA toont dan "Kan je selectie niet laden" en
                    // een favoriet selecteren werkt domweg niet meer.
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