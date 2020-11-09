package rs.lukaj.compressor.configuration

import org.springframework.stereotype.Component
import java.io.File

@Component
class EnvironmentProperties {

    fun getVideoQueueLocation() = File(getProperty("mc.queue.videos.path", "/opt/media-compressor/queue"))
    fun getMaxQueueSize() = getProperty("mc.queue.size", "5").toInt()
    fun getFreeSpaceThresholdMb() = getProperty("mc.space.threshold", "1500").toInt()

    fun getVideoTargetLocation() = File(getProperty("mc.videos.path", "/opt/media-compressor/results"))
    fun getVideoFormat() = getProperty("mc.video.format", "mp4")
    fun getVideoCodec() = getProperty("mc.video.codec", "libx265")
    fun getVideoFramerate() = getProperty("mc.video.framerate", "24").toDouble()
    fun getAudioCodec() = getProperty("mc.audio.codec", "aac")
    fun getAudioBitrate() = getProperty("mc.audio.bitrate", "24576").toLong()
    fun getAudioSamplerate() = getProperty("mc.audio.samplerate", "44100").toInt()

    fun getSendgridApiKey() = getProperty("mc.sendgrid.apikey", "")
    fun getMailSendingAddress() = getProperty("mc.sendgrid.from", "compressor@luka-j.rocks")
    fun getHostUrl() = getProperty("mc.host.url", "https://compressor.luka-j.rocks")

    fun getExecutorType() = getProperty("mc.executor.strategy", "single")

    private fun getProperty(property: String, default: String) : String {
        val envVarName = property.replace('.', '_').toUpperCase()
        val envVar : String? = System.getenv(envVarName)
        if(envVar != null) return envVar

        return System.getProperty(property, default)
    }
}