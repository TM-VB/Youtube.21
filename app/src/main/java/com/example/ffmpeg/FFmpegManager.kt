package com.example.ffmpeg

import android.content.Context
import android.os.Build
import com.example.downloader.ffmpeg.FFmpegManager as ProductionFFmpegManager

/**
 * Compatibility singleton providing access to system-wide FFmpeg initialization and device ABI info.
 * Unifies with and delegates to the production [com.example.downloader.ffmpeg.FFmpegManager].
 */
object FFmpegManager {

    private var isInitialized = false

    fun init(context: Context): Result<Unit> {
        if (isInitialized) return Result.success(Unit)
        return try {
            val prod = ProductionFFmpegManager.getInstance(context.applicationContext)
            val status = prod.getStatus()
            isInitialized = status.isAvailable
            Result.success(Unit)
        } catch (t: Throwable) {
            Result.failure(if (t is Exception) t else Exception(t.message, t))
        }
    }

    fun isReady(): Boolean = isInitialized

    fun getDeviceAbis(): List<String> {
        return Build.SUPPORTED_ABIS.toList()
    }

    fun getPrimaryAbi(): String {
        return Build.SUPPORTED_ABIS.firstOrNull() ?: "unknown"
    }
}
