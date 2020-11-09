package rs.lukaj.compressor.util

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component
import rs.lukaj.compressor.configuration.EnvironmentProperties
import java.util.*
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

@Component
class Utils(@Autowired private val properties : EnvironmentProperties) {
    private val logger = KotlinLogging.logger {}

    fun getQueueFreeSpaceMb() = properties.getVideoQueueLocation().usableSpace / (1024 * 1024)

    fun buildDownloadLink(id: UUID) = "${properties.getHostUrl()}/video/$id"

    fun getExecutor() : ExecutorService =
            when(val executorType = properties.getExecutorType()) {
                "single" -> Executors.newSingleThreadExecutor()
                "cached" -> Executors.newCachedThreadPool()
                "workstealing" -> Executors.newWorkStealingPool()
                else -> {
                    if(executorType.startsWith("fixed")) {
                        val threads = executorType.substring(5).toInt()
                        Executors.newFixedThreadPool(threads)
                    } else {
                        logger.warn { "Unrecognized executor type $executorType; using single thread executor" }
                        Executors.newSingleThreadExecutor()
                    }
                }
            }
}