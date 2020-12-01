package rs.lukaj.compressor.service

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rs.lukaj.compressor.configuration.EnvironmentProperties
import java.io.File
import java.io.InputStream
import java.util.*

@Service
class FileService(
        @Autowired private val properties: EnvironmentProperties
) {
    private val logger = KotlinLogging.logger {}

    fun saveVideoToQueue(videoId: UUID, stream: InputStream) : Long {
        val destFile = getQueueVideo(videoId)
        stream.copyTo(destFile.outputStream(), 131072)
        stream.close()
        return destFile.length()
    }

    fun getQueueVideo(videoId: UUID) : File {
        return File(properties.getVideoQueueLocation(), videoId.toString())
    }

    fun deleteQueueVideo(videoId: UUID, reason: String = "", errorLevel: String="WARN") {
        val file = getQueueVideo(videoId)
        delete(file, videoId, reason, errorLevel)
    }

    fun saveVideoToResults(videoId: UUID, stream: InputStream) : Long {
        val destFile = getResultVideo(videoId)
        stream.copyTo(destFile.outputStream(), 131072)
        stream.close()
        return destFile.length()
    }

    fun getResultVideo(videoId: UUID) : File {
        return File(properties.getVideoTargetLocation(), videoId.toString())
    }

    fun deleteResultVideo(videoId: UUID, reason: String = "", errorLevel: String = "WARN") {
        val file = getResultVideo(videoId)
        delete(file, videoId, reason, errorLevel)
    }

    fun getRecoveryVideos() : Array<File> {
        val recoveryDir = properties.getRecoveryQueueLocation()
        if(!recoveryDir.isDirectory) return arrayOf()
        return recoveryDir.listFiles()
    }

    private fun delete(file: File, videoId: UUID, reason: String, errorLevel: String) {
        if(!file.delete())
            when (errorLevel) {
                "WARN" -> logger.warn { "Failed to delete ${file.canonicalPath} of job $videoId" + (if(reason.isBlank()) "" else " ($reason)") }
                "DEBUG" -> logger.debug { "Failed to delete ${file.canonicalPath} of job $videoId" + (if(reason.isBlank()) "" else " ($reason)") }
                "INFO" -> logger.info { "Failed to delete ${file.canonicalPath} of job $videoId" + (if(reason.isBlank()) "" else " ($reason)") }
                else -> logger.error { "Failed to delete ${file.canonicalPath} of job $videoId" + (if(reason.isBlank()) "" else " ($reason)") }
            }
    }
}