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
class FileCleanup(
        @Autowired private val dao: VideoDao,
        @Autowired private val utils : Utils,
        @Autowired private val properties: EnvironmentProperties
) {
    private val logger = KotlinLogging.logger {}

    @Scheduled(cron = "0 */16 * * * *")
    fun removeClaimedFiles() {
        if(utils.getResultsFreeSpaceMb() > properties.getClaimedCleanupFreeSpaceThreshold()) return

        val videos = dao.getOldDownloadedVideos()
        if(videos.isEmpty()) {
            logger.warn { "We're below claimed threshold, but there're no claimed files to remove! " +
                    "Current free space: ${utils.getResultsFreeSpaceMb()}MB" }
        }

        for(video in videos) {
            val file = File(properties.getVideoTargetLocation(), video.name)
            if(!file.delete()) logger.error { "Failed to delete ${file.canonicalPath} (claimed and too old)" }
            dao.setVideoDeleted(video)
        }

        logger.info { "Claimed files cleanup: Removed ${videos.size} files, totalling ${videos.map { it.compressedSize }.sum() / (1024 * 1024)}MB" }
    }

    @Scheduled(cron = "0 */9 * * * *")
    fun removeUnclaimedFiles() {
        if(utils.getResultsFreeSpaceMb() > properties.getUnclaimedCleanupFreeSpaceThreshold()) return

        val videos = dao.getOldUndownloadedVideos()
        if(videos.isEmpty()) {
            logger.error { "We're below unclaimed threshold, but there're no claimed files to remove! " +
                    "Current free space: ${utils.getResultsFreeSpaceMb()}MB" }
        }

        for(video in videos) {
            logger.info { "Removing unclaimed file ${video.name} (id ${video.id}), meant for ${video.email}. " +
                    "Processed at ${video.updatedAt}, but not yet claimed." }
            val file = File(properties.getVideoTargetLocation(), video.name)
            if(!file.delete()) logger.error { "Failed to delete ${file.canonicalPath} (unclaimed but too old)" }
            dao.setVideoDeletedWithoutDownloading(video)
        }

        logger.info { "Unclaimed files cleanup: Removed ${videos.size} files, totalling ${videos.map { it.compressedSize }.sum() / (1024 * 1024)}MB" }
    }
}