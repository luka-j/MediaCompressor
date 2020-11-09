package rs.lukaj.compressor.model

import org.springframework.data.jpa.repository.JpaRepository
import org.springframework.stereotype.Repository
import java.util.*

@Repository
interface VideoRepository : JpaRepository<Video, UUID> {
    fun countAllByStatusIn(statuses: Collection<VideoStatus>) : Int
}