package rs.lukaj.compressor.service

import com.sendgrid.helpers.mail.objects.Email
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.dao.NODE_LOCAL
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
        @Autowired private val mailService : SendGridService,
        @Autowired private val workerService : WorkerService
) {
    private val logger = KotlinLogging.logger {}

    private val executor = utils.getExecutor()

    //@Transactional messes up stuff — record cannot be found in JPA repository from different thread
    fun addToQueue(file: InputStream, name: String, size: Long, email: String) {
        ensureQueueCanAcceptNewVideo(size)

        val videoInfo = prepareVideoForQueue(name, size, email, NODE_LOCAL, file)
        val destFile = videoInfo.second; val videoId = videoInfo.first

        //todo logic to delegate to workers if available
        executor.execute {
            converter.reencode(destFile, videoId)
            sendMailToUserIfNeeded(videoId, email)
        }
    }

    fun addVideoFromMaster(file: InputStream, originId: UUID, name: String, size: Long, origin: String, returnUrl: String) {
        ensureQueueCanAcceptNewVideo(size)

        val videoInfo = prepareVideoForQueue(name, size, "", origin, file, originId)
        val destFile = videoInfo.second; val videoId = videoInfo.first

        executor.execute {
            val result = converter.reencode(destFile, videoId)
            workerService.sendResultToMaster(originId, result, returnUrl)
            if(!result.delete()) logger.warn { "Failed to delete ${result.canonicalPath} after sending to master." }
            dao.setVideoDeleted(videoId)
        }
    }

    fun acceptProcessedVideo(file: InputStream, videoId: UUID) {
        val video = dao.getVideo(videoId).orElseThrow {
            logger.error { "Video $videoId not found, but we supposedly sent it to worker!" }
            EntityNotFound("Video with id $videoId not found.")
        }
        val destFile = File(properties.getVideoTargetLocation(), video.name)
        file.copyTo(destFile.outputStream(), 1048576)
        dao.setVideoProcessed(videoId, destFile.length())
        sendMailToUserIfNeeded(videoId, video.email)
    }

    fun getVideo(id: UUID) : File {
        val video = dao.getVideo(id).orElseThrow { EntityNotFound("Video with id $id not found.") }
        if(video.status != VideoStatus.READY && video.status != VideoStatus.DOWNLOADED) {
            throw InvalidStatus("Video not available: currently ${video.status}")
        }

        dao.setVideoDownloaded(id)
        return File(properties.getVideoTargetLocation(), video.name)
    }

    fun getQueueSize() = dao.getQueueSize()



    private fun sendMailToUserIfNeeded(videoId: UUID, email: String) {
        if(dao.getQueueSizeForEmail(email) == 0) {
            sendMailNotification(email)
        } else {
            logger.info { "Finished processing video $videoId for $email. Deferring email notification because there " +
                    "are more videos for this user in queue." }
            dao.setVideoMailPending(videoId)
        }
    }

    private fun ensureQueueCanAcceptNewVideo(size: Long) {
        if(utils.getQueueFreeSpaceMb()-size/(1024*1024) <= properties.getFreeSpaceThresholdMb()) throw NotEnoughSpace()
        if(dao.getQueueSize() >= properties.getMaxQueueSize()) throw QueueFull()
    }

    private fun prepareVideoForQueue(name: String, size: Long, email: String, origin: String, file: InputStream,
                                     originId: UUID? = null) : Pair<UUID, File> {
        val videoId = dao.createVideo(originId, name, size, email, origin)
        val destFile = File(properties.getVideoQueueLocation(), name)
        file.copyTo(destFile.outputStream(), 1048576)
        dao.setVideoUploaded(videoId, destFile.length())
        return Pair(videoId, destFile)
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