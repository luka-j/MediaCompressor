package rs.lukaj.compressor.model

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.data.jpa.repository.Modifying
import org.springframework.data.jpa.repository.Query
import org.springframework.stereotype.Repository
import java.time.LocalDateTime
import java.util.*

@Repository
interface VideoRepository : JpaRepository<Video, UUID> {
    fun countAllByStatusIn(statuses: Collection<VideoStatus>) : Int
    fun countAllByStatusInAndEmailEquals(statuses: Collection<VideoStatus>, email: String) : Int
    fun findAllByStatusEqualsAndEmailEquals(status: VideoStatus, email: String) : List<Video>

    fun findAllByStatusEqualsAndUpdatedAtBefore(status: VideoStatus, lastUpdate: LocalDateTime) : List<Video>

    @Modifying
    @Query("update Video v set v.status='READY' where v.email=:email")
    fun setVideosReadyForUser(email: String)
}

@Repository
interface WorkerRepository : JpaRepository<Worker, UUID> {
    fun findByHostEquals(host: String) : Optional<Worker>
}