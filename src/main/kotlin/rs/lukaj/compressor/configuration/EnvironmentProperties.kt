package rs.lukaj.compressor.configuration

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.springframework.stereotype.Component
import rs.lukaj.compressor.util.collapseIfEmpty
import rs.lukaj.compressor.util.nullIf
import java.io.File
import java.time.Instant

private const val DEFAULT_CONFIG_FILE_LOCATION = "/opt/media-compressor/config.conf"
@Component
class EnvironmentProperties {

    fun getVideoQueueLocation() = File(getProperty("queue.videos.path", "/opt/media-compressor/queue"))
    fun getMaxQueueSize() = getProperty("queue.size", "5").toInt()
    fun getFreeSpaceThresholdMb() = getProperty("space.threshold", "1500").toInt()
    fun getQueueMinimumSpaceRemaining() = getProperty("space.threshold.panic", "300").toInt()

    fun getVideoTargetLocation() = File(getProperty("videos.path", "/opt/media-compressor/results"))
    fun getVideoFormat() = getProperty("video.format", "mp4")
    fun getVideoCodec() = getProperty("video.codec", "libx265")
    fun getVideoFramerate() = getProperty("video.framerate", "24").toDouble()
    fun getVideoCRF() = getProperty("video.crf", "28").toDouble()
    fun getAudioCodec() = getProperty("audio.codec", "aac")
    fun getAudioBitrate() = getProperty("audio.bitrate", "32768").toLong()
    fun getAudioSamplerate() = getProperty("audio.samplerate", "44100").toInt()
    fun getAudioChannels() = getProperty("audio.channels", "1").toInt()

    fun getSendgridApiKey() = getProperty("sendgrid.apikey", "").nullIf("")
    fun getMailSendingAddress() = getProperty("sendgrid.from", "compressor@luka-j.rocks")
    fun getHostUrl() = getProperty("host.url", "https://compressor.luka-j.rocks")

    fun getExecutorType() = getProperty("executor.strategy", "single")
    fun getSecondaryExecutorType() = getProperty("executor2.strategy", "cached")
    fun getMaxConcurrentLocalJobs() = getProperty("workqueue.concurrent", "1").toInt()

    fun getClaimedCleanupFreeSpaceThreshold() = getProperty("cleanup.claimed.threshold", "2000").toInt()
    fun getUnclaimedCleanupFreeSpaceThreshold() = getProperty("cleanup.unclaimed.threshold", "4000").toInt()
    fun getClaimedCleanupTimeThreshold() = getProperty("cleanup.claimed.time", "15").toLong()
    fun getUnclaimedCleanupTimeThreshold() = getProperty("cleanup.unclaimed.time", "90").toLong()
    fun getZombieErrorCleanupFreeSpaceThreshold() = getProperty("cleanup.zombie.error.space", "6000").toInt()
    fun getZombieErrorCleanupTimeThreshold() = getProperty("cleanup.zombie.error.time", "45").toLong()
    fun getTransitiveStatusesCleanupTimeThreshold() = getProperty("cleanup.zombie.transitive.time", "15").toLong()
    fun getStaleVideosCleanupTimeThreshold() = getProperty("cleanup.zombie.stale.time", "720").toLong()
    fun getInQueueVideosCleanupTimeThreshold() = getProperty("cleanup.zombie.inqueue.time", "1440").toLong()

    fun isWorkerModeEnabled() = getProperty("worker.enabled", "true").toBoolean()
    fun getAllowedMasterHosts() = getProperty("worker.masters", "").split(",").collapseIfEmpty()
    fun getAvailableWorkers() : List<Pair<String, Double>> = getProperty("workers.available", "").split(",")
            .collapseIfEmpty().map { wrk ->
                val tokens = wrk.split(":")
                if(tokens.size == 1) Pair(tokens[0], 1.0)
                else Pair(tokens[0], tokens[1].toDouble())
            }
    fun getDownPingsThresholdToDeclareDead() = getProperty("worker.downpings.threshold", "3").toInt()
    fun getMyMasterKey() = getProperty("master.key", "").nullIf("")
    fun getSubmitWorkToMasterTimeout() = getProperty("worker.submit.timeout", "1800").toLong()
    fun getSubmitWorkToMasterRetryAttempts() = getProperty("worker.submit.retries", "5").toLong()
    fun getSubmitWorkToMasterMinBackoff() = getProperty("worker.submit.retry.minbackoff", "10").toLong()
    fun getSendWorkToWorkerTimeout() = getProperty("master.send.timeout", "1200").toLong()
    fun getSendWorkToWorkerTimeoutRetryAttempts() = getProperty("master.send.retries", "3").toLong()
    fun getSendWorkToWorkerTimeoutMinBackoff() = getProperty("master.send.retry.minbackoff", "5").toLong()
    fun getWorkerPingTimeout() = getProperty("worker.ping.timeout", "5").toLong()
    fun getQueueStatusRequestTimeout() = getProperty("worker.status.timeout", "5").toLong()
    fun getQueueIntegrityCheckTimeout() = getProperty("worker.queueintegrity.timeout", "10").toLong()

    fun getQueueFullResponseCachingTime() = getProperty("videoapi.queuefull.cache.time", "60").toLong()

    /**
     * Attempts to retrieve property from several locations. First, it attempts to find it in a file determined by
     * [EnvironmentProperties.getConfigFileLocation]. If not found, it prefixes the property with "mc." and attempts
     * to find that in System properties. If still not found, it uppercases the system property name, replaces dots
     * with underscores and attempts to find that among envrionment variables (so the variable starts with "MC_").
     * If no value is found in any of these three sources, returns default.
     */
    private fun getProperty(property: String, default: String) : String {
        val configProperty = getPropertyFromConfigFile(property)
        if(configProperty != null) return configProperty

        val systemProperty = "mc.$property"
        val jvmProperty : String? = System.getProperty(systemProperty, default)
        if(jvmProperty != null) return jvmProperty

        val envVarName = systemProperty.replace('.', '_').toUpperCase()
        val envVar : String? = System.getenv(envVarName)
        if(envVar != null) return envVar

        return default
    }


    private var currentConfig: Config? = null
    private var lastConfigUpdate: Instant = Instant.EPOCH

    private fun getPropertyFromConfigFile(property: String) : String? {
        val configFile = File(getConfigFileLocation(DEFAULT_CONFIG_FILE_LOCATION))
        if(!configFile.isFile) return null

        if(Instant.ofEpochMilli(configFile.lastModified()).isAfter(lastConfigUpdate)) {
            currentConfig = ConfigFactory.parseFile(configFile)
            lastConfigUpdate = Instant.now()
            if(currentConfig!!.hasPath("config.location"))
                System.setProperty("mc.config.location", currentConfig!!.getString("config.location"))
        }

        return if(currentConfig!!.hasPath(property)) currentConfig!!.getString(property)
        else null
    }

    private fun getConfigFileLocation(default: String) : String {
        val jvmProperty = System.getProperty("mc.config.location")
        if(jvmProperty != null) return jvmProperty

        val envVar : String? = System.getenv("MC_CONFIG_LOCATION")
        if(envVar != null) return envVar

        return default
    }
}