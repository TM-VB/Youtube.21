package com.example.downloader.recovery

import android.content.Context
import android.net.Uri
import com.example.data.local.DownloadTaskEntity
import com.example.data.repository.DownloadRepository
import com.example.domain.model.DownloadStatus
import java.io.File

/**
 * Handles database consistency verification and recovery of interrupted downloads
 * following process death, app crash, or system restart.
 */
class DownloadRecoveryManager(
    private val context: Context,
    private val repository: DownloadRepository
) {
    suspend fun recoverInterruptedDownloads() {
        try {
            repository.markActiveTasksAsInterrupted()
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    suspend fun verifyDatabaseConsistency() {
        try {
            val completedTasks = repository.getAllCompletedTasksSync()
            for (task in completedTasks) {
                val path = task.filePath
                val uriStr = task.contentUri
                var exists = false

                if (!path.isNullOrBlank()) {
                    exists = File(path).exists()
                }
                if (!exists && !uriStr.isNullOrBlank()) {
                    try {
                        val uri = Uri.parse(uriStr)
                        context.contentResolver.openInputStream(uri)?.use {
                            exists = true
                        }
                    } catch (_: Exception) {
                        exists = false
                    }
                }

                if (!exists) {
                    repository.updateTask(
                        task.copy(
                            status = DownloadStatus.FAILED,
                            errorMessage = "File missing from disk or was moved."
                        )
                    )
                }
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    suspend fun checkDuplicate(
        url: String,
        formatId: String,
        startTime: String?,
        endTime: String?
    ): DownloadTaskEntity? {
        return repository.findExistingTask(url, formatId, startTime, endTime)
    }
}
