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
        @Autowired private val mailService : SendGridGateway,
        @Autowired private val workerService : WorkerGateway,
        @Autowired private val queue : WorkQueue
) {
    private val logger = KotlinLogging.logger {}

    //@Transactional messes up stuff — record cannot be found in JPA repository from different thread
    fun addToQueue(file: InputStream, name: String, size: Long, email: String) {
        ensureEnoughFreeSpaceInQueue(size)

        val videoInfo = prepareVideoForQueue(name, size, email, NODE_LOCAL, file)
        val destFile = videoInfo.second; val videoId = videoInfo.first

        queue.addToQueue(Job(videoId, destFile, JobOrigin.LOCAL) {
            try {
                dao.setVideoProcessed(videoId, File(properties.getVideoTargetLocation(), destFile.name).length())
                sendMailToUserIfNeeded(videoId, email)
            } catch (e: Exception) {
                logger.error(e) { "Unexpected exception occurred while finalizing job $videoId; doing nothing" }
            }
        })
    }

    fun addVideoFromMaster(file: InputStream, originId: UUID, name: String, size: Long, origin: String, returnUrl: String) {
        ensureEnoughFreeSpaceInQueue(size)

        val videoInfo = prepareVideoForQueue(name, size, "", origin, file, originId)
        val queueFile = videoInfo.second; val videoId = videoInfo.first
        val resultFile = File(properties.getVideoTargetLocation(), name)

        queue.addToQueue(Job(videoId, queueFile, JobOrigin.REMOTE) {
            try {
                workerService.sendResultToMaster(originId, resultFile, returnUrl)
                if (!resultFile.delete()) logger.warn { "Failed to delete ${resultFile.canonicalPath} after sending to master." }
                dao.setVideoDeleted(videoId)
            } catch (e: Exception) {
                logger.error(e) { "Unexpected exception occurred while sending result to master; failing video $videoId" }
                dao.setVideoError(videoId)
            }
        })
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

    fun videoExists(id: UUID) : Boolean = dao.getVideo(id).isPresent

    fun getQueueSize() = dao.getQueueSize()

    fun checkQueueFull(requestOrigin: String, size: Long) {
        ensureEnoughFreeSpaceInQueue(size)
        if(queue.getQueueSize() < properties.getMaxQueueSize()) return
        val myMasterKey = properties.getMyMasterKey()
        if(requestOrigin == "" && myMasterKey == null) throw QueueFull()

        val originKey = if(requestOrigin == "") myMasterKey!! else requestOrigin
        for(worker in properties.getAvailableWorkers()) {
            try {
                if (!workerService.isQueueFull(worker, originKey)) return
            } catch (e: Exception) {
                //ignore, worker is unavailable / queue full / whatever; go to the next one
            }
        }
        throw QueueFull()
    }

    private fun sendMailToUserIfNeeded(videoId: UUID, email: String) {
        if(dao.getQueueSizeForEmail(email) == 0) {
            sendMailNotification(email)
        } else {
            logger.info { "Finished processing video $videoId for $email. Deferring email notification because there " +
                    "are more videos for this user in queue." }
            dao.setVideoMailPending(videoId)
        }
    }

    private fun ensureEnoughFreeSpaceInQueue(size: Long) {
        if(utils.getQueueFreeSpaceMb()-size/(1024*1024) <= properties.getQueueMinimumSpaceRemaining()) throw NotEnoughSpace()
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