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
        @Autowired private val dao: VideoDao,
        @Autowired private val files: FileService
) {
    private val logger = KotlinLogging.logger {}

    fun reencode(videoId: UUID) : File {
        logger.info { "Starting reencode of video $videoId" }

        val resultFile = files.getResultVideo(videoId)

        val ffmpeg = FFmpeg("/usr/bin/ffmpeg")
        val ffprobe = FFprobe("/usr/bin/ffprobe")

        val probe = ffprobe.probe(files.getQueueVideo(videoId).canonicalPath)
        val durationNs = probe.format.duration * TimeUnit.SECONDS.toNanos(1)

        val builder = FFmpegBuilder()
                .setInput(probe) // Filename, or a FFmpegProbeResult
                .overrideOutputFiles(true) // Override the output if it exists
                .addOutput(resultFile.canonicalPath) // Filename for the destination
                .setFormat(properties.getVideoFormat()) // Format is inferred from filename, or can be set
                .setAudioCodec(properties.getAudioCodec()) // using the aac codec
                .setAudioSampleRate(properties.getAudioSamplerate()) // at 44.1KHz
                .setAudioBitRate(properties.getAudioBitrate()) // at 32 kbit/s
                .setAudioChannels(properties.getAudioChannels()) // mono
                .setVideoCodec(properties.getVideoCodec()) // Video using x265
                .setConstantRateFactor(properties.getVideoCRF()) //
                .setVideoFrameRate(properties.getVideoFramerate()) // at 24 frames per second
                .setStrict(FFmpegBuilder.Strict.EXPERIMENTAL) // Allow FFmpeg to use experimental specs
                .done()

        val executor = FFmpegExecutor(ffmpeg, ffprobe)

        executor.createJob(builder) { progress ->
            dao.updateVideoProgress(videoId, ((progress.out_time_ns / durationNs) * 100).toInt(), progress.speed)
        }.run()

        return resultFile
    }
}