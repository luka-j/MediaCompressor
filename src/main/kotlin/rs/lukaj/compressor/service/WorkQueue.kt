package rs.lukaj.compressor.service

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.dao.VideoDao
import rs.lukaj.compressor.dao.WorkerDao
import rs.lukaj.compressor.model.WorkerStatus
import rs.lukaj.compressor.util.NotEnoughSpace
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
        @Autowired private val workerService : WorkerGateway
) {
    private val logger = KotlinLogging.logger {}

    private val lock = Any()
    private val mainExecutor = utils.getExecutor(properties.getExecutorType())
    private val shortTasksExecutor = utils.getExecutor(properties.getSecondaryExecutorType())

    private val queue = ConcurrentLinkedDeque<Job>()

    @Volatile private var jobsExecuting = 0

    fun addToQueue(job: Job) {
        queue.push(job)
        synchronized(lock) {
            if (jobsExecuting < properties.getMaxConcurrentJobs()) {
                jobsExecuting++
                val nextJob = queue.pop()
                execute(nextJob)
            }
        }
    }

    private fun nextJob() {
        synchronized(lock) {
            val job = queue.poll()
            if (job == null) return
            else {
                jobsExecuting++
                execute(job)
            }
        }
    }

    private fun execute(job: Job) {
        if(job.origin == JobOrigin.REMOTE) return executeLocally(job)

        var minQueueSize = dao.getQueueSize()
        var minQueueLocation = "localhost"
        //this is not exactly thread-safe (esp in distributed environment), but oh well
        for(worker in properties.getAvailableWorkers()) {
            val workerEntity = workerDao.getOrCreateWorker(worker)
            if(workerEntity.status == WorkerStatus.DOWN) continue
            try {
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

        if(minQueueLocation == "localhost") return executeLocally(job)
        else try {
            executeRemotely(job, minQueueLocation)
        } catch (e: Exception) {
            logger.warn { "Exception occurred while sending work to worker: ${e.javaClass.name}: ${e.message}. Executing job locally." }
            executeLocally(job)
        }
    }

    private fun executeLocally(job: Job) {
        ensureQueueCanAcceptNewVideo(job.queueFile.length())
        mainExecutor.execute {
            converter.reencode(job.queueFile, job.videoId)
            shortTasksExecutor.execute(job.finalizedBy)
            nextJob()
            jobsExecuting--
        }
    }
    private fun executeRemotely(job: Job, worker: String) {
        workerService.sendWorkToWorker(worker, job.queueFile.name, job.videoId, job.queueFile)
        jobsExecuting--
    }

    fun ensureQueueCanAcceptNewVideo(size: Long) {
        if(utils.getQueueFreeSpaceMb()-size/(1024*1024) <= properties.getFreeSpaceThresholdMb()) throw NotEnoughSpace()
        if(queue.size >= properties.getMaxQueueSize()) throw QueueFull()
    }

    fun getQueueSize() = queue.size
}

class Job(val videoId: UUID, val queueFile: File, val origin: JobOrigin, val finalizedBy: ()->Unit)
enum class JobOrigin { //locally originated job can be transferred to worker, remote job can't be transferred
    LOCAL,
    REMOTE
}