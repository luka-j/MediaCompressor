package rs.lukaj.compressor.scheduled

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.model.VideoStatus
import rs.lukaj.compressor.service.FileService
import rs.lukaj.compressor.service.VideoService
import rs.lukaj.compressor.util.Utils
import java.io.BufferedInputStream
import java.io.FileInputStream
import java.util.*
import javax.annotation.PostConstruct

@Service
class RecoveryScheduler(
        @Autowired private val utils : Utils,
        @Autowired private val properties: EnvironmentProperties,
        @Autowired private val service: VideoService,
        @Autowired private val files: FileService
) {
    private val logger = KotlinLogging.logger {}

    @PostConstruct //we want to run this right away as well
    @Scheduled(cron = "0 */19 * * * *")
    fun rescheduleVideosForRecovery() {
        //this assumes filenames are video IDs (UUIDs) and as such is very brittle
        val filesForRecovery = files.getRecoveryVideos()
        for(file in filesForRecovery) {
            val video = service.getVideo(UUID.fromString(file.name))
            if(video.isEmpty || video.get().status != VideoStatus.ERROR) {
                logger.warn { "Invalid video $video: won't attempt recovery" }
                continue
            }
            val fis = BufferedInputStream(FileInputStream(file))
            try {
                service.addToQueue(fis, video.get().name, video.get().originalSize, video.get().email)
                file.delete()
            } catch (e: Exception) {
                logger.warn(e) { "Exception occurred while attempting to add video for recovery" }
            }
        }
    }
}