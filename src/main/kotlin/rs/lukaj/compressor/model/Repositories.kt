package rs.lukaj.compressor.model

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.stereotype.Repository
import java.time.LocalDateTime
import java.util.*

@Repository
interface VideoRepository : JpaRepository<Video, UUID> {
    fun findByOriginId(originId: UUID) : Optional<Video>

    fun countAllByStatusIn(statuses: Collection<VideoStatus>) : Int
    fun countAllByStatusInAndEmailEquals(statuses: Collection<VideoStatus>, email: String) : Int
    fun findAllByStatusInAndEmailEquals(statuses: Collection<VideoStatus>, email: String) : List<Video>
    fun findAllByStatusNotInAndEmailEquals(statuses: Collection<VideoStatus>, email: String) : List<Video>

    fun findAllByStatusEqualsAndUpdatedAtBefore(status: VideoStatus, lastUpdate: LocalDateTime) : List<Video>
    fun findAllByStatusInAndUpdatedAtBefore(statuses: Collection<VideoStatus>, lastUpdate: LocalDateTime) : List<Video>
    fun findAllByStatusNotInAndUpdatedAtBefore(statuses: Collection<VideoStatus>, lastUpdate: LocalDateTime) : List<Video>
    fun findAllByStatusEqualsAndNodeEquals(status: VideoStatus, node: String) : List<Video>
    fun findAllByStatusEqualsAndNodeNot(status: VideoStatus, node: String) : List<Video>

    @Modifying
    @Query("update Video v set v.status='READY' where v.email=:email and v.status in ('PROCESSED', 'EMAIL_PENDING')")
    fun setVideosReadyForUser(email: String)
}

@Repository
interface WorkerRepository : JpaRepository<Worker, UUID> {
    fun findByHostEquals(host: String) : Optional<Worker>
}