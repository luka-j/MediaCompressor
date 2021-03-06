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
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.locks.ReentrantLock

@Service
class WorkQueue(
        @Autowired private val properties: EnvironmentProperties,
        @Autowired private val utils: Utils,
        @Autowired private val converter: VideoConverter,
        @Autowired private val dao: VideoDao,
        @Autowired private val workerDao: WorkerDao,
        @Autowired private val workerService : WorkerGateway,
        @Autowired private val files : FileService
) {
    private val logger = KotlinLogging.logger {}

    private val lock = ReentrantLock()
    private val mainExecutor = utils.getExecutor(properties.getExecutorType())
    private val shortTasksExecutor = utils.getExecutor(properties.getSecondaryExecutorType())

    private val queue = ConcurrentLinkedDeque<Job>()

    @Volatile private var jobsExecuting = 0

    fun addToQueue(job: Job, onJobFailed: (UUID)->Unit, bypassSizeCheck: Boolean = false) {
        try {
            queue.push(job)
            nextJob(onJobFailed, bypassSizeCheck)
        } catch (e: QueueFull) {
            queue.pollLast()
            throw e
        }
    }

    fun nextJob(onJobFailed: (UUID)->Unit, bypassSizeCheck: Boolean = false) {
        logger.info { "Advancing queue by a single job. Lock isLocked: ${lock.isLocked}" }
        lock.lock()
        try {
            val job = queue.peek()
            if (job == null) {
                lock.unlock()
                return
            }
            else {
                execute(job, bypassSizeCheck, onJobFailed)
            }
        } catch (e: Exception) {
            if(lock.isHeldByCurrentThread) {
                lock.unlock()
            }
            throw e
        }
    }

    //call this when a new worker comes alive, to assign new work to it
    fun resetQueue(onJobFailed: (UUID) -> Unit) {
        lock.lock()
        try {
            val queueSize = queue.size
            for(i in 1..queueSize) {
                val top = queue.pop()
                addToQueue(top, onJobFailed)
                lock.lock()
            }
        } finally {
            lock.unlock()
        }
    }

    private fun execute(job: Job, bypassSizeCheck: Boolean, onJobFailed: (UUID)->Unit) {
        if(job.origin == JobOrigin.REMOTE) return executeLocally(job, bypassSizeCheck, onJobFailed)

        val eligibleNodes = TreeSet<Pair<String, Double>>(Comparator.comparingDouble { it.second })
        try {
            ensureQueueCanAcceptNewVideo()
            eligibleNodes.add(Pair(NODE_LOCAL, getQueueSize() * 1.0))
        } catch (e: Exception) {
            logger.info { "NODE_LOCAL won't compete for ${job.videoId}: ${e::class.simpleName}" }
        }
        //this is not exactly thread-safe (esp in distributed environment), but oh well, I'm not going to implement distributed locks

        if(properties.getMyMasterKey() != null) {
            for ((host, efficiency) in properties.getAvailableWorkers()) {
                try {
                    val workerEntity = workerDao.getOrCreateWorker(host)
                    if (workerEntity.status == WorkerStatus.DOWN) continue
                    val status = workerService.getQueueStatus(host)
                    workerDao.setWorkerQueueSize(workerEntity, status.size)
                    if (status.isDiskFull) continue
                    if (status.size >= status.maxSize) continue
                    eligibleNodes.add(Pair(host, status.size * 1/efficiency))
                } catch (e: Exception) {
                    logger.info { "Got ${e.javaClass.name}: ${e.message} while fetching worker status, not sending work to it" }
                    continue
                }
            }
        }

        for(node in eligibleNodes) {
            try {
                return if(node.first == NODE_LOCAL) executeLocally(job, bypassSizeCheck, onJobFailed)
                else executeRemotely(job, node.first)
            } catch (e: Exception) {
                logger.warn { "Exception occurred attempting to execute ${job.videoId} on ${node.first}. Moving on..." }
            }
        }

        lock.unlock()
        logger.warn { "No available workers for job ${job.videoId}. Rejecting." }
        dao.setVideoRejected(job.videoId)
        files.deleteQueueVideo(job.videoId, "rejected job")
        throw QueueFull()
    }

    private fun executeLocally(job: Job, bypassSizeCheck: Boolean, onJobFailed: (UUID)->Unit) {
        if(!bypassSizeCheck) ensureQueueCanAcceptNewVideo()
        dao.setVideoInQueue(job.videoId)
        if (jobsExecuting < properties.getMaxConcurrentLocalJobs()) {
            jobsExecuting++
            logger.info { "Executing job ${job.videoId} locally..." }
            val nextJob = queue.pop()
            mainExecutor.execute {
                try {
                    dao.setVideoProcessing(nextJob.videoId, NODE_LOCAL)
                    converter.reencode(nextJob.videoId)
                    shortTasksExecutor.execute(nextJob.finalizedBy)
                } catch (e: Exception) {
                    logger.error(e) { "Unexpected exception occurred while executing reencode job; failing video ${job.videoId}" }
                    onJobFailed(nextJob.videoId)
                }
                jobsExecuting--
                nextJob(onJobFailed)
            }
        } else {
            logger.info { "Added job ${job.videoId} work queue. Position: ${queue.size}." }
        }
        lock.unlock()
    }

    private fun executeRemotely(job: Job, worker: String) {
        if(properties.getMyMasterKey() == null || properties.getHostUrl() == null) {
            logger.error { "Attempted to send job to worker, but this instance doesn't have master key set! Add master key to config." }
            throw RemoteExecutionInvocationException("Cannot execute job remotely without master key!")
        }
        logger.info { "Executing job ${job.videoId} on $worker..." }
        workerService.sendWorkToWorker(worker, job.videoId)
        queue.pop() //pop job only after we're sure execution has started
        lock.unlock()
        dao.setVideoProcessing(job.videoId, worker)
    }

    fun ensureQueueCanAcceptNewVideo() {
        if(queue.size > properties.getMaxQueueSize()) throw QueueFull()
    }

    fun getQueueSize() = queue.size
}

class Job(val videoId: UUID, val origin: JobOrigin, val finalizedBy: ()->Unit)
enum class JobOrigin { //locally originated job can be transferred to worker, remote job can't be transferred
    LOCAL,
    REMOTE
}

class RemoteExecutionInvocationException(msg: String) : Exception(msg)