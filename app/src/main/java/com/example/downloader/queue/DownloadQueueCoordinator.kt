package com.example.downloader.queue

import com.example.data.repository.DownloadRepository
import com.example.data.settings.AppSettings
import com.example.downloader.network.NetworkMonitor
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.concurrent.ConcurrentHashMap

/**
 * Coordinates concurrency limits, queue slots, and network availability.
 * Decides which queued tasks to dispatch for execution.
 */
class DownloadQueueCoordinator(
    private val repository: DownloadRepository,
    private val appSettings: AppSettings,
    private val networkMonitor: NetworkMonitor,
    private val activeJobs: ConcurrentHashMap<String, Job>,
    private val scope: CoroutineScope,
    private val onStartTask: suspend (taskId: String) -> Unit
) {
    private val queueMutex = Mutex()
    private val _activeDownloadCount = MutableStateFlow(0)
    val activeDownloadCount: StateFlow<Int> = _activeDownloadCount.asStateFlow()

    fun updateActiveCount(count: Int) {
        _activeDownloadCount.value = count
    }

    suspend fun processQueue() {
        queueMutex.withLock {
            // Purge any inactive/completed/cancelled jobs to guarantee accurate active counts
            activeJobs.entries.removeIf { !it.value.isActive }

            val maxConcurrency = appSettings.concurrentDownloads.value.coerceIn(1, 3)
            val currentActiveCount = activeJobs.size
            val availableSlots = maxConcurrency - currentActiveCount

            _activeDownloadCount.value = currentActiveCount

            if (availableSlots <= 0) {
                return
            }

            if (!networkMonitor.isOnline()) {
                return
            }

            val queuedTasks = repository.getQueuedTasks()
                .filter { task ->
                    val job = activeJobs[task.id]
                    job == null || !job.isActive
                }
                .distinctBy { it.id }

            val tasksToStart = queuedTasks.take(availableSlots)

            for (task in tasksToStart) {
                val existingJob = activeJobs[task.id]
                if (existingJob == null || !existingJob.isActive) {
                    val job = scope.launch(start = kotlinx.coroutines.CoroutineStart.LAZY) {
                        try {
                            onStartTask(task.id)
                        } catch (t: Throwable) {
                            if (t is kotlinx.coroutines.CancellationException) {
                                throw t
                            }
                        }
                    }
                    job.invokeOnCompletion {
                        if (activeJobs.remove(task.id, job)) {
                            _activeDownloadCount.value = activeJobs.size
                            scope.launch { processQueue() }
                        }
                    }
                    activeJobs[task.id] = job
                    _activeDownloadCount.value = activeJobs.size
                    job.start()
                }
            }
        }
    }
}
