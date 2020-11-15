package rs.lukaj.compressor.dao

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.model.Video
import rs.lukaj.compressor.model.VideoRepository
import rs.lukaj.compressor.model.VideoStatus
import rs.lukaj.compressor.util.InternalServerError
import java.time.LocalDateTime
import java.util.*
import kotlin.collections.HashMap

val IN_QUEUE_STATES = listOf(VideoStatus.UPLOADING, VideoStatus.UPLOADED, VideoStatus.IN_QUEUE, VideoStatus.PROCESSING)
//states which should quickly be changed; not something that should last long
private val TRANSITIVE_STATES = listOf(VideoStatus.UPLOADED, VideoStatus.PROCESSED)
//not including ERROR here  vvv  on purpose
private val FINAL_STATES = listOf(VideoStatus.REJECTED, VideoStatus.DELETED, VideoStatus.DELETED_WITHOUT_DOWNLOADING, VideoStatus.ERROR)
const val NODE_LOCAL = "localhost"

@Service
class VideoDao(
        @Autowired private val repository: VideoRepository,
        @Autowired private val properties: EnvironmentProperties
) {
    private val logger = KotlinLogging.logger {}
    private val progressCache = HashMap<UUID, Int>()


    fun getQueueSize() : Int {
        return repository.countAllByStatusIn(IN_QUEUE_STATES)
    }

    fun getQueueSizeForEmail(email: String) : Int {
        return repository.countAllByStatusInAndEmailEquals(IN_QUEUE_STATES, email)
    }

    fun createVideo(name: String, size: Long, email: String, origin: String, originId: UUID?) : Video {
        val video = Video(null, name, email, size, 0, 0, .0f,
                VideoStatus.UPLOADING, NODE_LOCAL, origin, originId)
        return repository.save(video)
    }

    fun setVideoUploaded(id: UUID, size: Long) {
        val video = findOrThrow(id)
        video.originalSize = size
        video.status = VideoStatus.UPLOADED
        repository.save(video)
    }

    fun setVideoInQueue(id: UUID) = setVideoStatus(id, VideoStatus.IN_QUEUE)

    fun setVideoProcessing(id: UUID, node: String) {
        val video = findOrThrow(id)
        video.status = VideoStatus.PROCESSING
        video.node = node
        repository.save(video)
    }

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
    @Transactional
    fun setVideosReady(email: String) = repository.setVideosReadyForUser(email)

    fun getAllPendingVideosForUser(email: String) =
            repository.findAllByStatusInAndEmailEquals(listOf(VideoStatus.EMAIL_PENDING, VideoStatus.PROCESSED), email)
    fun getAllVideosForUser(email: String) = repository.findAllByStatusNotInAndEmailEquals(FINAL_STATES, email)

    fun getVideo(id: UUID) : Optional<Video> = repository.findById(id)

    fun getVideoByOriginId(id: UUID) : Optional<Video> = repository.findByOriginId(id)

    fun setVideoDownloaded(id: UUID) = setVideoStatus(id, VideoStatus.DOWNLOADED)
    fun setVideoDownloaded(video: Video) = setVideoStatus(video, VideoStatus.DOWNLOADED)

    fun getOldDownloadedVideos() : List<Video> =
            repository.findAllByStatusEqualsAndUpdatedAtBefore(VideoStatus.DOWNLOADED,
                    LocalDateTime.now().minusMinutes(properties.getClaimedCleanupTimeThreshold()))
    fun getOldUndownloadedVideos() : List<Video> =
            repository.findAllByStatusEqualsAndUpdatedAtBefore(VideoStatus.READY,
                    LocalDateTime.now().minusMinutes(properties.getUnclaimedCleanupTimeThreshold()))
    fun getOldErrorZombieVideos() : List<Video> =
            repository.findAllByStatusEqualsAndUpdatedAtBefore(VideoStatus.ERROR,
                    LocalDateTime.now().minusMinutes(properties.getZombieErrorCleanupTimeThreshold()))
    fun getOldTransitiveStatusZombieVideos() : List<Video> =
            repository.findAllByStatusInAndUpdatedAtBefore(TRANSITIVE_STATES,
                    LocalDateTime.now().minusMinutes(properties.getTransitiveStatusesCleanupTimeThreshold()))
    fun getStaleVideos() : List<Video> =
            repository.findAllByStatusNotInAndUpdatedAtBefore(FINAL_STATES,
                    LocalDateTime.now().minusMinutes(properties.getStaleVideosCleanupTimeThreshold()))

    fun setVideoDeleted(id: UUID) = setVideoStatus(id, VideoStatus.DELETED)
    fun setVideoDeleted(video: Video) = setVideoStatus(video, VideoStatus.DELETED)
    fun setVideoDeletedWithoutDownloading(video: Video) = setVideoStatus(video, VideoStatus.DELETED_WITHOUT_DOWNLOADING)

    fun setVideoError(id: UUID) = setVideoStatus(id, VideoStatus.ERROR)
    fun setVideoRejected(id: UUID) = setVideoStatus(id, VideoStatus.REJECTED)

    fun getVideosProcessingOnNode(node: String) = repository.findAllByStatusEqualsAndNodeEquals(VideoStatus.PROCESSING, node)

    fun getVideosProcessingOnWorkers() = repository.findAllByStatusEqualsAndNodeNot(VideoStatus.PROCESSING, NODE_LOCAL)

    private fun setVideoStatus(id: UUID, status: VideoStatus) : Video = setVideoStatus(findOrThrow(id), status)
    private fun setVideoStatus(video: Video, status: VideoStatus) : Video {
        video.status = status
        return repository.save(video)
    }

    private fun findOrThrow(id: UUID) : Video {
        return repository.findById(id).orElseThrow {
            logger.error { "Attempted to find video with id $id, but nothing was found!" }
            InternalServerError()
        }
    }
}