package rs.lukaj.compressor.dao

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rs.lukaj.compressor.model.Video
import rs.lukaj.compressor.model.VideoRepository
import rs.lukaj.compressor.model.VideoStatus
import rs.lukaj.compressor.util.InternalServerError
import java.util.*
import kotlin.collections.HashMap

@Service
class VideoDao(@Autowired val repository: VideoRepository) {
    private val logger = KotlinLogging.logger {}
    private val progressCache = HashMap<UUID, Int>()

    fun getQueueSize() : Int {
        return repository.countAllByStatusIn(listOf(VideoStatus.UPLOADING, VideoStatus.UPLOADED, VideoStatus.PROCESSING))
    }

    fun createVideo(name: String, size: Long, email: String) : UUID {
        val video = Video(null, name, email, size, 0, VideoStatus.UPLOADING);
        return repository.save(video).id!!
    }

    fun setVideoUploaded(id: UUID) {
        setVideoStatus(id, VideoStatus.UPLOADED)
    }

    fun setVideoProcessing(id: UUID) {
        setVideoStatus(id, VideoStatus.PROCESSING)
    }

    fun updateVideoProgress(id: UUID, newProgress: Int) {
        if(progressCache[id] != null && progressCache[id] == newProgress) return

        repository.findById(id).map {
            it.reencodeProgress = newProgress
            repository.save(it)
            progressCache[id] = newProgress
            it
        }.or {
            logger.warn { "Attempted to find video with id $id to update progress, but nothing was found!" }
            Optional.empty<Video>()
        }
    }

    fun setVideoProcessed(id: UUID) {
        setVideoStatus(id, VideoStatus.PROCESSED)
        progressCache.remove(id)
    }

    fun setVideoReady(id: UUID) {
        setVideoStatus(id, VideoStatus.READY)
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