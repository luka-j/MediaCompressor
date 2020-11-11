package rs.lukaj.compressor.service

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.util.Utils
import java.io.File
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque

@Service
class WorkQueue(
        @Autowired private val properties: EnvironmentProperties,
        @Autowired private val utils: Utils,
        @Autowired private val converter: VideoConverter,
        @Autowired private val workerService : WorkerGateway
) {
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
                executeLocally(nextJob)
                //todo execute on worker, if available and origin is LOCAL
                // (endpoint to see current queue size, max queue size and free space status)
            }
        }
    }

    private fun nextJob() {
        synchronized(lock) {
            val job = queue.pop()
            if (job == null) return
            else {
                jobsExecuting++
                executeLocally(job)
            }
        }
    }

    private fun executeLocally(job: Job) {
        mainExecutor.execute {
            converter.reencode(job.queueFile, job.videoId)
            shortTasksExecutor.execute(job.finalizedBy)
            jobsExecuting--
            nextJob()
        }
    }
}

class Job(val videoId: UUID, val queueFile: File, val origin: JobOrigin, val finalizedBy: ()->Unit)
enum class JobOrigin { //locally originated job can be transferred to worker, remote job can't be transferred
    LOCAL,
    REMOTE
}