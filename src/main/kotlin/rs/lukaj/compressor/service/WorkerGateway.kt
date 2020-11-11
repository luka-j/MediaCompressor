package rs.lukaj.compressor.service

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.client.HttpServerErrorException
import org.springframework.web.reactive.function.BodyExtractors
import org.springframework.web.reactive.function.client.WebClient
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.controller.FILE_NAME_HEADER
import rs.lukaj.compressor.controller.MASTER_KEY_HEADER
import rs.lukaj.compressor.controller.RETURN_URL_HEADER
import rs.lukaj.compressor.controller.VIDEO_ID_HEADER
import rs.lukaj.compressor.dto.QueueSizeResponse
import rs.lukaj.compressor.util.addTrailingSlash
import java.io.File
import java.io.OutputStream
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.*


@Service
class WorkerGateway(
    @Autowired private val properties: EnvironmentProperties
) {
    private val logger = KotlinLogging.logger {}
    private val client = WebClient.create()

    fun sendResultToMaster(originId: UUID, video: File, returnUrl: String) {
        val response = client.post()
                .uri(returnUrl)
                .header(VIDEO_ID_HEADER, originId.toString())
                .body(video.outputStream(), OutputStream::class.java)
                .exchange()
                .retry(5)
                .blockOptional(Duration.ofSeconds(properties.getSubmitWorkToMasterTimeout()))
                .orElseThrow {
                    logger.error { "Error occurred while submitting work to master!" }
                    HttpTimeoutException("Request timed out or retried too many times!")
                }

        if(response.statusCode() != HttpStatus.OK) {
            logger.error { "Error occurred while submitting work to master! Got code ${response.statusCode()} and body: " +
                    "${response.bodyToMono(String::class.java).block(Duration.ofSeconds(4))}" }
            throw HttpServerErrorException(response.statusCode(), "Error while submitting work to master!")
        }
    }

    fun sendWorkToWorker(host: String, filename: String, videoId: UUID, file: File) {
        val response = client.post()
                .uri(host.addTrailingSlash() + "worker/")
                .header(MASTER_KEY_HEADER, properties.getMyMasterKey())
                .header(FILE_NAME_HEADER, filename)
                .header(VIDEO_ID_HEADER, videoId.toString())
                .header(RETURN_URL_HEADER, properties.getHostUrl().addTrailingSlash() + "worker/accept")
                .body(file.outputStream(), OutputStream::class.java)
                .exchange()
                .retry(3)
                .blockOptional(Duration.ofSeconds(properties.getSendWorkToWorkerTimeout()))
                .orElseThrow {
                    logger.error { "Error occurred while sending work to worker!" }
                    HttpTimeoutException("Request timed out or retried too many times!")
                }
        if(response.statusCode() != HttpStatus.ACCEPTED) {
            logger.error { "Error occurred while sending work to worker! Got code ${response.statusCode()} and body: " +
                    "${response.bodyToMono(String::class.java).block(Duration.ofSeconds(4))}" }
            throw HttpServerErrorException(response.statusCode(), "Error while sending work to worker!")
        }
    }

    fun ping(host: String) : Boolean {
        val response = client.get()
                .uri(host.addTrailingSlash() + "worker/ping")
                .header(MASTER_KEY_HEADER, properties.getMyMasterKey())
                .exchange()
                .block(Duration.ofSeconds(properties.getWorkerPingTimeout()))
                ?: return false
        if(response.statusCode() != HttpStatus.OK) return false
        return true
    }

    fun isQueueFull(host: String, originKey: String) : Boolean {
        val response = client.get()
                .uri(host.addTrailingSlash() + "video/queue/status")
                .header(MASTER_KEY_HEADER, originKey)
                .exchange()
                .block(Duration.ofSeconds(properties.getWorkerPingTimeout()))
                ?: return true
        response.releaseBody()
        return response.statusCode() != HttpStatus.OK
    }

    fun getQueueStatus(host: String) : QueueSizeResponse {
        return client.get()
                .uri(host.addTrailingSlash() + "worker/queue/size")
                .header(MASTER_KEY_HEADER, properties.getMyMasterKey())
                .exchange()
                .blockOptional(Duration.ofSeconds(properties.getQueueStatusRequestTimeout()))
                .orElseThrow {
                    logger.error { "Error occurred while checking status of worker $host" }
                    HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR)
                }
                .body(BodyExtractors.toMono(QueueSizeResponse::class.java))
                .blockOptional(Duration.ofSeconds(properties.getQueueStatusRequestTimeout()))
                .orElseThrow {
                    logger.error { "Error occurred while checking status of worker $host" }
                    HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR)
                }
    }
}