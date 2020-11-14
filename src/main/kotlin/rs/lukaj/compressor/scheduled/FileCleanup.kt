package rs.lukaj.compressor.scheduled

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.dao.VideoDao
import rs.lukaj.compressor.service.FileService
import rs.lukaj.compressor.util.Utils

@Service
class FileCleanup(
        @Autowired private val dao: VideoDao,
        @Autowired private val utils : Utils,
        @Autowired private val properties: EnvironmentProperties,
        @Autowired private val files: FileService
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
            files.deleteResultVideo(video.id!!, "claimed and too old", "ERROR")
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
            files.deleteResultVideo(video.id!!, "unclaimed but too old", "ERROR")
            dao.setVideoDeletedWithoutDownloading(video)
        }

        logger.info { "Unclaimed files cleanup: Removed ${videos.size} files, totalling ${videos.map { it.compressedSize }.sum() / (1024 * 1024)}MB" }
    }
}