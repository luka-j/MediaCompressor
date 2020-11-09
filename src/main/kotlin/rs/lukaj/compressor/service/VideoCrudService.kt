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
        dao.setVideoUploaded(videoId, destFile.length())
        executor.execute {
            converter.reencode(destFile, videoId)
            if(dao.getQueueSizeForEmail(email) == 0) {
                sendMailNotification(email)
            } else {
                logger.info { "Finished processing video $videoId for $email. Deferring email notification because there " +
                        "are more videos for this user in queue." }
                dao.setVideoMailPending(videoId)
            }
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


    private fun sendMailNotification(recipient: String) {
        val videos = dao.getAllPendingVideosForUser(recipient)

        //I could've used some proper templating here, but seemed like an overkill
        if(videos.size == 1) {
            mailService.sendMail("Kompresovan snimak je spreman!", "Tvoj snimak ${videos[0].name} je spreman. " +
                    "Možeš ga preuzeti <a href=\"${utils.buildDownloadLink(videos[0].id!!)}\">ovde</a>", true, Email(recipient))
        } else {
            val msg = StringBuilder("Tvoji snimci su spremni. Možeš ih naći na sledećim linkovima:<br><ul>")
            for(video in videos) {
                msg.append("<li><a href=\"${utils.buildDownloadLink(video.id!!)}\">${video.name}</a></li>")
            }
            msg.append("</ul>")
            mailService.sendMail("Kompresovani snimci su spremni!", msg.toString(), true, Email(recipient))
        }

        dao.setVideosReady(recipient)
    }

}