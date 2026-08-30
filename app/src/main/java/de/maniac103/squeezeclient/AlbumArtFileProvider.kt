/*
 * This file is part of Squeeze Client, an Android client for the LMS music server.
 * Copyright (c) 2024 Danny Baumann
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 *  GNU General Public License as published by the Free Software Foundation,
 *   either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 */

package de.maniac103.squeezeclient

import android.content.ContentProvider
import android.content.ContentValues
import android.content.res.AssetFileDescriptor
import android.database.Cursor
import android.database.MatrixCursor
import android.net.Uri
import android.os.ParcelFileDescriptor
import android.provider.OpenableColumns
import java.io.File
import java.io.FileNotFoundException

// Exported provider exposing only the cached album art thumbnails in cacheDir/album_art_cache/.
// Android Auto/AAOS resolve artwork URIs in MediaMetadata via ContentResolver from their own
// process, which requires this provider to be exported; androidx FileProvider refuses that.
class AlbumArtFileProvider : ContentProvider() {
    override fun onCreate() = true

    private fun resolveFile(uri: Uri): File {
        val fileName = uri.lastPathSegment
            ?: throw FileNotFoundException("No filename in $uri")
        val cacheDir = File(requireContext().cacheDir, "album_art_cache")
        val file = File(cacheDir, fileName)
        // Prevent path traversal outside cacheDir/album_art_cache.
        if (file.parentFile?.canonicalFile != cacheDir.canonicalFile) {
            throw FileNotFoundException("Invalid path: $uri")
        }
        return file
    }

    override fun openFile(uri: Uri, mode: String): ParcelFileDescriptor {
        val file = resolveFile(uri)
        if (!file.isFile) throw FileNotFoundException(uri.toString())
        return ParcelFileDescriptor.open(file, ParcelFileDescriptor.MODE_READ_ONLY)
    }

    override fun openAssetFile(uri: Uri, mode: String): AssetFileDescriptor {
        val file = resolveFile(uri)
        if (!file.isFile) throw FileNotFoundException(uri.toString())
        return AssetFileDescriptor(openFile(uri, mode), 0, file.length())
    }

    override fun getType(uri: Uri) = "image/jpeg"

    override fun query(
        uri: Uri,
        projection: Array<out String>?,
        selection: String?,
        selectionArgs: Array<out String>?,
        sortOrder: String?
    ): Cursor? {
        val file = runCatching { resolveFile(uri) }.getOrNull()?.takeIf { it.isFile }
            ?: return null
        val columns = projection ?: arrayOf(OpenableColumns.DISPLAY_NAME, OpenableColumns.SIZE)
        val cursor = MatrixCursor(columns)
        val row = columns.map { column ->
            when (column) {
                OpenableColumns.DISPLAY_NAME -> file.name
                OpenableColumns.SIZE -> file.length()
                else -> null
            }
        }
        cursor.addRow(row)
        return cursor
    }

    override fun insert(uri: Uri, values: ContentValues?): Uri? =
        throw UnsupportedOperationException()

    override fun update(
        uri: Uri,
        values: ContentValues?,
        selection: String?,
        selectionArgs: Array<out String>?
    ) = throw UnsupportedOperationException()

    override fun delete(uri: Uri, selection: String?, selectionArgs: Array<out String>?) =
        throw UnsupportedOperationException()
}
