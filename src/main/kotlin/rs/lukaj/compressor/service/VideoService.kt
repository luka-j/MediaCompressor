package rs.lukaj.compressor.service

import com.sendgrid.helpers.mail.objects.Email
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.dao.IN_QUEUE_STATES
import rs.lukaj.compressor.dao.NODE_LOCAL
import rs.lukaj.compressor.dao.VideoDao
import rs.lukaj.compressor.model.Video
import rs.lukaj.compressor.model.VideoStatus
import rs.lukaj.compressor.util.EntityNotFound
import rs.lukaj.compressor.util.InvalidStatus
import rs.lukaj.compressor.util.NotEnoughSpace
import rs.lukaj.compressor.util.Utils
import java.io.File
import java.io.InputStream
import java.util.*

@Service
class VideoService(
        @Autowired private val properties: EnvironmentProperties,
        @Autowired private val dao: VideoDao,
        @Autowired private val utils: Utils,
        @Autowired private val mailService : SendGridGateway,
        @Autowired private val workerService : WorkerGateway,
        @Autowired private val queue : WorkQueue,
        @Autowired private val files : FileService
) {
    private val logger = KotlinLogging.logger {}

    //@Transactional messes up stuff — record cannot be found in JPA repository from different thread
    fun addToQueue(file: InputStream, name: String, size: Long, email: String) {
        ensureEnoughFreeSpaceInQueue(size)

        val video = prepareVideoForQueue(name, size, email, NODE_LOCAL, file)

        queue.addToQueue(buildLocallyOriginatedJob(video), {failJob(it)})
    }

    fun addVideoFromMaster(file: InputStream, originId: UUID, name: String, size: Long, origin: String, returnUrl: String) {
        ensureEnoughFreeSpaceInQueue(size)

        val video = prepareVideoForQueue(name, size, "", origin, file, originId)
        val videoId = video.id!!

        queue.addToQueue(Job(videoId, JobOrigin.REMOTE) {
            try {
                workerService.sendResultToMaster(videoId, originId, returnUrl)
                files.deleteQueueVideo(videoId, "after submitting to master")
                files.deleteResultVideo(videoId, "after submitting to master")
                dao.setVideoDeleted(videoId)
            } catch (e: Exception) {
                logger.error(e) { "Unexpected exception occurred while sending result to master; failing video $videoId" }
                failJob(videoId)
            }
        }, {failJob(it)})
    }

    fun acceptProcessedVideo(file: InputStream, videoId: UUID) {
        val video = dao.getVideo(videoId).orElseThrow {
            logger.error { "Video $videoId not found, but we supposedly sent it to worker!" }
            EntityNotFound("Video with id $videoId not found.")
        }
        dao.setVideoProcessed(videoId, files.saveVideoToResults(videoId, file))
        files.deleteQueueVideo(videoId, "after receiving result from worker")
        sendMailToUserIfNeeded(videoId, video.email)

        try {
            queue.nextJob({failJob(it)})
        } catch (e: Exception) {
            logger.info { "Attempted to move queue after accepting a job result from worker, but failed: ${e.javaClass.simpleName}" }
        }
    }

    fun reassignVideo(video: Video) {
        queue.addToQueue(buildLocallyOriginatedJob(video), {failJob(it)},true)
    }

    fun reassignWorkFromDeadNode(node: String) {
        dao.getVideosProcessingOnNode(node).forEach { video -> reassignVideo(video) }
    }

    fun getVideo(id: UUID) : Optional<Video> {
        return dao.getVideo(id)
    }
    fun getVideosForUser(email: String) : List<Video> = dao.getAllVideosForUser(email)

    fun downloadVideo(video: Video) : File {
        if(video.status != VideoStatus.READY && video.status != VideoStatus.DOWNLOADED) {
            throw InvalidStatus("Video not available: currently ${video.status}")
        }

        dao.setVideoDownloaded(video)
        return files.getResultVideo(video.id!!)
    }

    fun videoExists(id: UUID) : Boolean = dao.getVideo(id).isPresent

    fun videoFromRemoteExistsInQueue(id: UUID) : Boolean = dao.getVideoByOriginId(id)
            .map { v -> IN_QUEUE_STATES.contains(v.status) }.orElse(false)

    fun getQueueSize() = dao.getQueueSize()

    fun isQueueFull(requestOrigin: String, size: Long) : Boolean {
        if(isThereEnoughFreeSpaceInQueue(size) && queue.getQueueSize() < properties.getMaxQueueSize()) return false
        val myMasterKey = properties.getMyMasterKey()
        if(requestOrigin == "" && myMasterKey == null) return true

        val originKey = if(requestOrigin == "") myMasterKey!! else requestOrigin
        for((worker, efficiency) in properties.getAvailableWorkers()) {
            try {
                if (!workerService.isQueueFull(worker, originKey)) return false
            } catch (e: Exception) {
                //ignore, worker is unavailable / queue full / whatever; go to the next one
            }
        }
        return true
    }

    fun failJob(videoId: UUID) {
        logger.info { "Failing video $videoId and notifying user." }
        val video = dao.setVideoError(videoId)
        if(video.email.isBlank()) {
            logger.warn { "User cannot be notified of failure $videoId - mail is blank (this was probably a remote job)" }
            return
        }
        mailService.sendMail("An error occurred while processing your video.", "An error occurred while " +
                "processing your video ${video.name}. This is the id: ${video.id}, so drop me a message. Sorry :/",
                false, Email(video.email))
        if(dao.getAllPendingVideosForUser(video.email).isNotEmpty()) {
            logger.info { "There are some pending videos for user ${video.email}. Sending notifications..." }
            sendMailNotification(video.email)
        }
    }

    private fun buildLocallyOriginatedJob(video: Video) : Job {
        val videoId = video.id!!
        return Job(videoId, JobOrigin.LOCAL) {
            try {
                dao.setVideoProcessed(videoId, files.getResultVideo(videoId).length())
                files.deleteQueueVideo(videoId, "after processing")
                sendMailToUserIfNeeded(videoId, video.email)
            } catch (e: Exception) {
                logger.error(e) { "Unexpected exception occurred while finalizing job $videoId; doing nothing" }
            }
        }
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

    private fun isThereEnoughFreeSpaceInQueue(size: Long) : Boolean {
        val res = utils.getQueueFreeSpaceMb()-size/(1024*1024)-reservedSpace/(1024*1024) > properties.getQueueMinimumSpaceRemaining()
        if(!res) {
            logger.warn { "Not enough space: total free space ${utils.getQueueFreeSpaceMb()}, this video size:" +
                    "${size/(1024*1024)}, reserved: ${reservedSpace/(1024*1024)}, and threshold is ${properties.getQueueMinimumSpaceRemaining()}" }
        }
        return res;
    }
    private fun ensureEnoughFreeSpaceInQueue(size: Long) {
        if(!isThereEnoughFreeSpaceInQueue(size)) throw NotEnoughSpace()
    }

    private fun prepareVideoForQueue(name: String, size: Long, email: String, origin: String, file: InputStream,
                                     originId: UUID? = null) : Video {
        val video = dao.createVideo(name, size, email, origin, originId)
        reserveSpace(size)
        try {
            val realSize = files.saveVideoToQueue(video.id!!, file)
            dao.setVideoUploaded(video.id!!, realSize)
        } finally {
            unreserveSpace(size)
        }
        return video
    }

    internal fun sendMailNotification(recipient: String) {
        val videos = dao.getAllPendingVideosForUser(recipient)

        //I could've used some proper templating here, but seemed like an overkill
        if(videos.size == 1) {
            mailService.sendMail("Compressed video is ready!", "Your video ${videos[0].name} is ready. " +
                    "You can download it <a href=\"${utils.buildDownloadLink(videos[0].id!!)}\">here</a>", true, Email(recipient))
        } else {
            val msg = StringBuilder("Your videos are ready. You can download them using these links:<br><ul>")
            for(video in videos) {
                msg.append("<li><a href=\"${utils.buildDownloadLink(video.id!!)}\">${video.name}</a></li>")
            }
            msg.append("</ul>")
            mailService.sendMail("Compressed videos are ready!", msg.toString(), true, Email(recipient))
        }

        dao.setVideosReady(recipient)
    }


    //this part is ugly and race condition-prone
    private var reservedSpace = 0L
    private fun reserveSpace(space: Long) {
        reservedSpace += space
    }
    private fun unreserveSpace(space: Long) {
        reservedSpace -= space
        if(reservedSpace < 0) {
            logger.warn { "Reserved space appears to be < 0: $reservedSpace. Something is wrong!" }
            reservedSpace = 0;
        }
    }
}