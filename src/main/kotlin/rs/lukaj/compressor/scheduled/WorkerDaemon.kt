package rs.lukaj.compressor.scheduled

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.dao.VideoDao
import rs.lukaj.compressor.dao.WorkerDao
import rs.lukaj.compressor.model.WorkerStatus
import rs.lukaj.compressor.service.VideoCrudService
import rs.lukaj.compressor.service.WorkQueue
import rs.lukaj.compressor.service.WorkerGateway

@Service
class WorkerDaemon(
        @Autowired private val properties: EnvironmentProperties,
        @Autowired private val dao : WorkerDao,
        @Autowired private val workerService: WorkerGateway,
        @Autowired private val queue: WorkQueue,
        @Autowired private val videoService: VideoCrudService,
        @Autowired private val videoDao: VideoDao
) {
    private val logger = KotlinLogging.logger {}

    private val maybeDownWorkers = HashSet<String>()

    @Scheduled(fixedDelay = 60_000, initialDelay = 0)
    fun pingWorkers() {
        val workerHosts = properties.getAvailableWorkers()
        var newWorkersAreUp = false
        for((host, efficiency) in workerHosts) {
            val worker = dao.getOrCreateWorker(host)
            val isUp = try {
                workerService.ping(host)
            } catch (e: Exception) {
                false
            }
            if((worker.status == WorkerStatus.UP) == isUp) { //status has not changed
                if(isUp) continue
                else if(worker.downPings >= properties.getDownPingsThresholdToDeclareDead()) continue
            }

            dao.setWorkerStatus(worker, if(isUp) WorkerStatus.UP else WorkerStatus.DOWN)
            if(!isUp) {
                maybeDownWorkers.add(host)
                if(worker.downPings >= properties.getDownPingsThresholdToDeclareDead()) {
                    maybeDownWorkers.remove(host)
                    if (worker.queueSize <= 0) continue
                    videoService.reassignWorkFromDeadNode(host)
                }
            } else {
                if(maybeDownWorkers.contains(host)) {
                    maybeDownWorkers.remove(host)
                    val queueStatus = workerService.getQueueStatus(host)
                    if(queueStatus.size != videoDao.getVideosProcessingOnNode(host).size) {
                        //if queue was impacted, reassign all work (potentially to same worker)
                        videoService.reassignWorkFromDeadNode(host)
                    }
                } else {
                    newWorkersAreUp = true
                }
            }
        }
        if(newWorkersAreUp) {
            queue.resetQueue { videoService.failJob(it) }
        }
    }

    @Scheduled(cron = "0 */18 * * * *")
    fun checkWorkerQueueIntegrity() {
        val videosOnWorkers = videoDao.getVideosProcessingOnWorkers()
        for(video in videosOnWorkers) {
            val isVideoInQueue = try {
                workerService.isVideoInQueue(video.node, video.id!!)
            } catch (e: Exception) {
                false
            }

            if(!isVideoInQueue) {
                logger.warn { "Video ${video.id} (${video.name}) not found in queue of ${video.node}! Readding to queue." }
                videoService.reassignVideo(video)
            }
        }
    }
}