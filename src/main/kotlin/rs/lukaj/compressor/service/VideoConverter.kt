package rs.lukaj.compressor.service

import mu.KotlinLogging
import net.bramp.ffmpeg.FFmpeg
import net.bramp.ffmpeg.FFmpegExecutor
import net.bramp.ffmpeg.FFprobe
import net.bramp.ffmpeg.builder.FFmpegBuilder
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.dao.VideoDao
import java.io.File
import java.util.*
import java.util.concurrent.TimeUnit


@Service
class VideoConverter(
        @Autowired private val properties: EnvironmentProperties,
        @Autowired private val dao: VideoDao
) {
    private val logger = KotlinLogging.logger {}

    fun reencode(file: File, videoId: UUID) : File {
        logger.info { "Starting reencode of video $videoId" }
        dao.setVideoProcessing(videoId)

        val resultFile = File(properties.getVideoTargetLocation(), file.name)

        val ffmpeg = FFmpeg("/usr/bin/ffmpeg")
        val ffprobe = FFprobe("/usr/bin/ffprobe")

        val probe = ffprobe.probe(file.canonicalPath)
        val durationNs = probe.format.duration * TimeUnit.SECONDS.toNanos(1)

        val builder = FFmpegBuilder()
                .setInput(probe) // Filename, or a FFmpegProbeResult
                .overrideOutputFiles(true) // Override the output if it exists
                .addOutput(resultFile.canonicalPath) // Filename for the destination
                .setFormat(properties.getVideoFormat()) // Format is inferred from filename, or can be set
                .setAudioCodec(properties.getAudioCodec()) // using the aac codec
                .setAudioSampleRate(properties.getAudioSamplerate()) // at 44.1KHz
                .setAudioBitRate(properties.getAudioBitrate()) // at 24 kbit/s
                .setVideoCodec(properties.getVideoCodec()) // Video using x265
                .setVideoFrameRate(properties.getVideoFramerate()) // at 24 frames per second
                .setStrict(FFmpegBuilder.Strict.EXPERIMENTAL) // Allow FFmpeg to use experimental specs
                .done()

        val executor = FFmpegExecutor(ffmpeg, ffprobe)

        executor.createJob(builder) { progress ->
            logger.info { "Video transcoding speed: ${progress.speed}" }
            dao.updateVideoProgress(videoId, (progress.out_time_ns / durationNs).toInt())
        }.run()

        dao.setVideoProcessed(videoId)
        if(!file.delete()) logger.warn { "Failed to delete file ${file.canonicalPath} (video $videoId)!" }
        return resultFile
    }
}