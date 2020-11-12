package rs.lukaj.compressor.service

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.dao.NODE_LOCAL
import rs.lukaj.compressor.dao.VideoDao
import rs.lukaj.compressor.dao.WorkerDao
import rs.lukaj.compressor.model.WorkerStatus
import rs.lukaj.compressor.util.QueueFull
import rs.lukaj.compressor.util.Utils
import java.io.File
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque

@Service
class WorkQueue(
        @Autowired private val properties: EnvironmentProperties,
        @Autowired private val utils: Utils,
        @Autowired private val converter: VideoConverter,
        @Autowired private val dao: VideoDao,
        @Autowired private val workerDao: WorkerDao,
        @Autowired private val workerService : WorkerGateway,
        @Autowired private val videoService: VideoCrudService
) {
    private val logger = KotlinLogging.logger {}

    private val lock = Any()
    private val mainExecutor = utils.getExecutor(properties.getExecutorType())
    private val shortTasksExecutor = utils.getExecutor(properties.getSecondaryExecutorType())

    private val queue = ConcurrentLinkedDeque<Job>()

    @Volatile private var jobsExecuting = 0

    fun addToQueue(job: Job, bypassSizeCheck: Boolean = false) {
        try {
            queue.push(job)
            nextJob(bypassSizeCheck)
        } catch (e: QueueFull) {
            queue.pollLast()
            throw e
        }
    }

    fun nextJob(bypassSizeCheck: Boolean = false) {
        synchronized(lock) {
            val job = queue.peek()
            if (job == null) return
            else {
                execute(job, bypassSizeCheck)
            }
        }
    }

    //call this when a new worker comes alive, to assign new work to it
    fun resetQueue() {
        synchronized(lock) { //this potentially locks queue for a while
            val queueSize = queue.size
            for(i in 1..queueSize) {
                val top = queue.pop()
                addToQueue(top)
            }
        }
    }

    private fun execute(job: Job, bypassSizeCheck: Boolean) {
        if(job.origin == JobOrigin.REMOTE) return executeLocally(job, bypassSizeCheck)

        var minQueueLocation = NODE_LOCAL
        var minQueueSize = try {
            ensureQueueCanAcceptNewVideo()
            dao.getQueueSize()
        } catch (e: Exception) {
            Int.MAX_VALUE
        }
        //this is not exactly thread-safe (esp in distributed environment), but oh well, I'm not going to implement distributed locks
        for(worker in properties.getAvailableWorkers()) {
            try {
                val workerEntity = workerDao.getOrCreateWorker(worker)
                if(workerEntity.status == WorkerStatus.DOWN) continue
                val status = workerService.getQueueStatus(worker)
                workerDao.setWorkerQueueSize(workerEntity, status.size)
                if(status.isDiskFull) continue
                if(status.size >= status.maxSize) continue
                if(status.size < minQueueSize) {
                    minQueueSize = status.size
                    minQueueLocation = worker
                }
            } catch (e: Exception) {
                logger.info { "Got ${e.javaClass.name}: ${e.message} while fetching worker status, not sending work to it" }
                continue
            }
        }

        synchronized(lock) {
            if (minQueueLocation == "localhost") return executeLocally(job, bypassSizeCheck)
            else try {
                executeRemotely(job, minQueueLocation)
            } catch (e: Exception) {
                logger.warn { "Exception occurred while sending work to worker: ${e.javaClass.name}: ${e.message}. Executing job locally." }
                executeLocally(job, bypassSizeCheck)
            }
        }
    }

    private fun executeLocally(job: Job, bypassSizeCheck: Boolean) {
        synchronized(lock) {
            if(!bypassSizeCheck) ensureQueueCanAcceptNewVideo()
            if (jobsExecuting < properties.getMaxConcurrentLocalJobs()) {
                jobsExecuting++
                val nextJob = queue.pop()
                dao.setVideoInQueue(nextJob.videoId)
                mainExecutor.execute {
                    try {
                        dao.setVideoProcessing(nextJob.videoId, NODE_LOCAL)
                        converter.reencode(nextJob.queueFile, nextJob.videoId)
                        shortTasksExecutor.execute(nextJob.finalizedBy)
                    } catch (e: Exception) {
                        logger.error(e) { "Unexpected exception occurred while executing reencode job; failing video ${job.videoId}" }
                        videoService.failJob(nextJob.videoId)
                    }
                    jobsExecuting--
                    nextJob()
                }
            }
        }
    }

    private fun executeRemotely(job: Job, worker: String) {
        workerService.sendWorkToWorker(worker, job.queueFile.name, job.videoId, job.queueFile)
        queue.pop() //pop job only after we're sure execution has started
        dao.setVideoProcessing(job.videoId, NODE_LOCAL)
    }

    fun ensureQueueCanAcceptNewVideo() {
        if(queue.size > properties.getMaxQueueSize()) throw QueueFull()
    }

    fun getQueueSize() = queue.size
}

class Job(val videoId: UUID, val queueFile: File, val origin: JobOrigin, val finalizedBy: ()->Unit)
enum class JobOrigin { //locally originated job can be transferred to worker, remote job can't be transferred
    LOCAL,
    REMOTE
}