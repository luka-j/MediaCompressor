package rs.lukaj.compressor.service

import com.sendgrid.helpers.mail.objects.Email
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.dao.VideoDao
import rs.lukaj.compressor.model.VideoStatus
import rs.lukaj.compressor.util.*
import java.io.File
import java.io.InputStream
import java.util.*

@Service
class VideoCrudService(
        @Autowired private val properties: EnvironmentProperties,
        @Autowired private val dao: VideoDao,
        @Autowired private val utils: Utils,
        @Autowired private val converter: VideoConverter,
        @Autowired private val mailService : SendGridService
) {
    private val logger = KotlinLogging.logger {}

    private val executor = utils.getExecutor()

    @Transactional
    fun addToQueue(file: InputStream, name: String, size: Long, email: String) {
        if(utils.getQueueFreeSpaceMb()-size/(1024*1024) <= properties.getFreeSpaceThresholdMb()) throw NotEnoughSpaceException()
        if(dao.getQueueSize() >= properties.getMaxQueueSize()) throw QueueFullException()

        val videoId = dao.createVideo(name, size, email)
        val destFile = File(properties.getVideoQueueLocation(), name)
        file.copyTo(destFile.outputStream(), 1048576)
        dao.setVideoUploaded(videoId)
        executor.execute {
            converter.reencode(destFile, videoId)
            sendMailNotification(videoId, name, email)
        }
    }

    @Transactional(readOnly = true)
    fun getVideo(id: UUID) : File {
        @Suppress("ThrowableNotThrown") val video = dao.getVideo(id).orElseThrow { EntityNotFoundException("Video with id $id not found.") }
        if(video.status != VideoStatus.READY && video.status != VideoStatus.DOWNLOADED) {
            throw InvalidStatusException("Video not available: currently ${video.status}")
        }

        return File(properties.getVideoTargetLocation(), video.name)
    }


    private fun sendMailNotification(videoId: UUID, name: String, recipient: String) {
        mailService.sendMail("Kompresovan snimak je spreman!", "Tvoj snimak $name je spreman. " +
                "Možeš ga preuzeti <a href=\"${utils.buildDownloadLink(videoId)}\">ovde</a>", true, Email(recipient))

        dao.setVideoReady(videoId)
    }

}