package rs.lukaj.compressor.util

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import rs.lukaj.compressor.configuration.EnvironmentProperties
import java.util.*

@Component
class Utils(@Autowired private val properties : EnvironmentProperties) {

    fun getQueueFreeSpaceMb() = properties.getVideoQueueLocation().usableSpace / (1024 * 1024)

    fun buildDownloadLink(id: UUID) = "https://compressor.luka-j.rocks/video/$id"
}