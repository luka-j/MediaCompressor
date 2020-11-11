package rs.lukaj.compressor.configuration

import org.springframework.stereotype.Component
import rs.lukaj.compressor.util.nullIf
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

    fun getSendgridApiKey() = getProperty("mc.sendgrid.apikey", "").nullIf("")
    fun getMailSendingAddress() = getProperty("mc.sendgrid.from", "compressor@luka-j.rocks")
    fun getHostUrl() = getProperty("mc.host.url", "https://compressor.luka-j.rocks")

    fun getExecutorType() = getProperty("mc.executor.strategy", "single")
    fun getSecondaryExecutorType() = getProperty("mc.executor2.strategy", "cached")
    fun getMaxConcurrentJobs() = getProperty("mc.workqueue.concurrent", "1").toInt()

    fun getClaimedCleanupFreeSpaceThreshold() = getProperty("mc.cleanup.claimed.threshold", "2000").toInt()
    fun getUnclaimedCleanupFreeSpaceThreshold() = getProperty("mc.cleanup.unclaimed.threshold", "4000").toInt()
    fun getClaimedCleanupTimeThreshold() = getProperty("mc.cleanup.claimed.time", "15").toLong()
    fun getUnclaimedCleanupTimeThreshold() = getProperty("mc.cleanup.unclaimed.time", "90").toLong()

    fun isWorkerModeEnabled() = getProperty("mc.worker.enabled", "true").toBoolean()
    fun getAllowedMasterHosts() = getProperty("mc.worker.masters", "*").split(",")
    fun getAvailableWorkers() : List<String> {
        val workers = getProperty("mc.workers.available", "")
        return if(workers == "") listOf() else workers.split(",")
        //apparently, split on empty string returns list of length 1
    }
    fun getMyMasterKey() = getProperty("mc.master.key", "").nullIf("")
    fun getSubmitWorkToMasterTimeout() = getProperty("mc.worker.submit.timeout", "1800").toLong()
    fun getWorkerPingTimeout() = getProperty("mc.worker.ping.timeout", "5").toLong()

    private fun getProperty(property: String, default: String) : String {
        val envVarName = property.replace('.', '_').toUpperCase()
        val envVar : String? = System.getenv(envVarName)
        if(envVar != null) return envVar

        return System.getProperty(property, default)
    }

    //todo runtime-modifiable config
}