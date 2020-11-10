package rs.lukaj.compressor.dao

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.model.Video
import rs.lukaj.compressor.model.VideoRepository
import rs.lukaj.compressor.model.VideoStatus
import rs.lukaj.compressor.util.InternalServerError
import java.time.LocalDateTime
import java.util.*
import kotlin.collections.HashMap

private val IN_QUEUE_STATUSES = listOf(VideoStatus.UPLOADING, VideoStatus.UPLOADED, VideoStatus.PROCESSING)
const val NODE_LOCAL = "localhost"

@Service
class VideoDao(
        @Autowired private val repository: VideoRepository,
        @Autowired private val properties: EnvironmentProperties
) {
    private val logger = KotlinLogging.logger {}
    private val progressCache = HashMap<UUID, Int>()


    fun getQueueSize() : Int {
        return repository.countAllByStatusIn(IN_QUEUE_STATUSES)
    }

    fun getQueueSizeForEmail(email: String) : Int {
        return repository.countAllByStatusInAndEmailEquals(IN_QUEUE_STATUSES, email)
    }

    fun createVideo(name: String, size: Long, email: String, origin: String) : UUID {
        val video = Video(null, name, email, size, 0, 0, .0f,
                VideoStatus.UPLOADING, NODE_LOCAL, origin)
        return repository.save(video).id!!
    }

    fun setVideoUploaded(id: UUID, size: Long) {
        val video = findOrThrow(id)
        video.originalSize = size
        video.status = VideoStatus.UPLOADED
        repository.save(video)
    }

    fun setVideoProcessing(id: UUID) = setVideoStatus(id, VideoStatus.PROCESSING)

    fun updateVideoProgress(id: UUID, newProgress: Int, speed: Float) {
        if(progressCache[id] != null && progressCache[id] == newProgress) return

        repository.findById(id).map {
            it.transcodingProgress = newProgress
            it.transcodingSpeed = speed
            repository.save(it)
            progressCache[id] = newProgress
            it
        }.or {
            logger.warn { "Attempted to find video with id $id to update progress, but nothing was found!" }
            Optional.empty<Video>()
        }
    }

    fun setVideoProcessed(id: UUID, compressedSize: Long) {
        val video = findOrThrow(id)
        video.compressedSize = compressedSize
        video.status = VideoStatus.PROCESSED
        repository.save(video)
        progressCache.remove(id)
    }

    fun setVideoMailPending(id: UUID) = setVideoStatus(id, VideoStatus.EMAIL_PENDING)
    fun setVideoReady(id: UUID) = setVideoStatus(id, VideoStatus.READY)
    fun setVideosReady(email: String) = repository.setVideosReadyForUser(email)

    fun getAllPendingVideosForUser(email: String) =
            repository.findAllByStatusEqualsAndEmailEquals(VideoStatus.EMAIL_PENDING, email)

    fun getVideo(id: UUID) : Optional<Video> = repository.findById(id)

    fun setVideoDownloaded(id: UUID) = setVideoStatus(id, VideoStatus.DOWNLOADED)

    fun getOldDownloadedVideos() : List<Video> =
            repository.findAllByStatusEqualsAndUpdatedAtBefore(VideoStatus.DOWNLOADED,
                    LocalDateTime.now().minusMinutes(properties.getClaimedCleanupTimeThreshold()))
    fun getOldUndownloadedVideos() : List<Video> =
            repository.findAllByStatusEqualsAndUpdatedAtBefore(VideoStatus.READY,
                    LocalDateTime.now().minusMinutes(properties.getUnclaimedCleanupTimeThreshold()))

    fun setVideoDeleted(video: Video) {
        video.status = VideoStatus.DELETED
        repository.save(video)
    }
    fun setVideoDeletedWithoutDownloading(video: Video) {
        video.status = VideoStatus.DELETED_WITHOUT_DOWNLOADING
        repository.save(video)
    }

    private fun setVideoStatus(id: UUID, status: VideoStatus) {
        val video = findOrThrow(id)
        video.status = status
        repository.save(video)
    }

    private fun findOrThrow(id: UUID) : Video {
        return repository.findById(id).orElseThrow {
            logger.error { "Attempted to find video with id $id, but nothing was found!" }
            InternalServerError()
        }
    }
}