package rs.lukaj.compressor.scheduled

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.dao.VideoDao
import rs.lukaj.compressor.util.Utils
import java.io.File

@Service
class ZombieVideoCleanup(
        @Autowired private val dao: VideoDao,
        @Autowired private val utils : Utils,
        @Autowired private val properties: EnvironmentProperties
) {
    private val logger = KotlinLogging.logger {}

    @Scheduled(cron = "*/15 * * * *")
    fun cleanupZombieErrors() {
        if(utils.getQueueFreeSpaceMb() > properties.getZombieErrorCleanupFreeSpaceThreshold() &&
                utils.getResultsFreeSpaceMb() > properties.getZombieErrorCleanupFreeSpaceThreshold()) return

        val zombies = dao.getOldErrorZombieVideos()
        for(video in zombies) {
            logger.info { "Removing zombie (ERROR) ${video.id} (${video.name})" }
            File(properties.getVideoQueueLocation(), video.name).delete()
            File(properties.getVideoTargetLocation(), video.name).delete()
        }
    }

    @Scheduled(cron = "*/13 * * * *")
    fun cleanupJobsStuckInTransitiveStates() {
        val zombies = dao.getOldTransitiveStatusZombieVideos()
        for(video in zombies) {
            logger.error { "Video ${video.id} (${video.name}) stuck in ${video.status} too long; marking it as ERROR." }
            dao.setVideoError(video.id!!)
        }
    }

    //todo remove files for other states, if it has not been touched for a long time and space is low (?)
}