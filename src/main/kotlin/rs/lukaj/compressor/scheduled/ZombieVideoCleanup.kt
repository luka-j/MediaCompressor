package rs.lukaj.compressor.scheduled

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.dao.VideoDao
import rs.lukaj.compressor.model.VideoStatus
import rs.lukaj.compressor.service.FileService
import rs.lukaj.compressor.service.VideoService
import rs.lukaj.compressor.util.Utils
import java.time.LocalDateTime

@Service
class ZombieVideoCleanup(
        @Autowired private val dao: VideoDao,
        @Autowired private val utils : Utils,
        @Autowired private val properties: EnvironmentProperties,
        @Autowired private val service: VideoService,
        @Autowired private val files: FileService
) {
    private val logger = KotlinLogging.logger {}

    @Scheduled(cron = "0 */15 * * * *")
    fun cleanupZombieErrors() {
        if(utils.getQueueFreeSpaceMb() > properties.getZombieErrorCleanupFreeSpaceThreshold() &&
                utils.getResultsFreeSpaceMb() > properties.getZombieErrorCleanupFreeSpaceThreshold()) return

        val zombies = dao.getOldErrorZombieVideos()
        for(video in zombies) {
            logger.info { "Removing zombie (ERROR) ${video.id} (${video.name})" }
            files.deleteQueueVideo(video.id!!, "marked as ERROR in database", "DEBUG")
            files.deleteResultVideo(video.id!!, "marked as ERROR in database", "DEBUG")
        }
    }

    @Scheduled(cron = "0 */13 * * * *")
    fun cleanupJobsStuckInTransitiveStates() {
        val zombies = dao.getOldTransitiveStatusZombieVideos()
        for(video in zombies) {
            logger.error { "Video ${video.id} (${video.name}) stuck in ${video.status} too long; marking it as ERROR." }
            service.failJob(video.id!!)
        }
    }

    @Scheduled(cron = "0 3 * * * *")
    fun cleanupStaleVideos() {
        val zombies = dao.getStaleVideos()
        val emailPendingCleared = HashSet<String>()
        for(video in zombies) {
            if(video.status == VideoStatus.EMAIL_PENDING) {
                if(!emailPendingCleared.contains(video.email)) {
                    service.sendMailNotification(video.email)
                    emailPendingCleared.add(video.email)
                } //else nothing - this video is READY and updatedAt is set to now when previous notification was sent
                //the only reason we caught it here is because zombies list is not updated to reflect changes that
                //happened in service.sendMailNotification(String)
            } else if (video.status != VideoStatus.IN_QUEUE || video.updatedAt!!.isBefore(
                            LocalDateTime.now().minusMinutes(properties.getInQueueVideosCleanupTimeThreshold()))) {
                logger.error { "Video ${video.id} (${video.name}) stuck in ${video.status} too long; marking it as ERROR." }
                service.failJob(video.id!!)
            }
        }
    }
}