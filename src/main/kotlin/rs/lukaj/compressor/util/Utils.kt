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
    fun getResultsFreeSpaceMb() = properties.getVideoTargetLocation().usableSpace / (1024 * 1024)

    fun buildDownloadLink(id: UUID) = "${properties.getHostUrl()}/video/$id"

    fun getExecutor(type: String) : ExecutorService =
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


fun String.nullIf(other: String) : String? = if(this == other) null else this
fun String.addTrailingSlash() = if(this.endsWith('/')) this else "$this/"
fun String.replaceFileExtension(newExtension: String) : String {
    if(this.endsWith(".$newExtension")) return this
    val lastPeriod = this.lastIndexOf('.')
    return if(lastPeriod == 1 || lastPeriod < this.length - 5) {
        "$this.$newExtension"
    } else {
        this.substring(0..lastPeriod) + newExtension
    }
}
fun List<String>.collapseIfEmpty() : List<String> = if(this.size == 1 && this[0] == "") listOf() else this