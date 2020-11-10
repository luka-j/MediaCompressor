package rs.lukaj.compressor.service

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.client.HttpServerErrorException
import org.springframework.web.reactive.function.client.WebClient
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.controller.MASTER_KEY_HEADER
import rs.lukaj.compressor.controller.VIDEO_ID_HEADER
import java.io.File
import java.io.OutputStream
import java.net.http.HttpTimeoutException
import java.time.Duration
import java.util.*


@Service
class WorkerService(
    @Autowired private val properties: EnvironmentProperties
) {
    private val logger = KotlinLogging.logger {}

    fun sendResultToMaster(originId: UUID, video: File, returnUrl: String) {

        val client = WebClient.create()
        val response = client.post()
                .uri(returnUrl)
                .header(VIDEO_ID_HEADER, originId.toString())
                .header(MASTER_KEY_HEADER, properties.getMyMasterKey())
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
                    "${response.bodyToMono(String::class.java).block(Duration.ofSeconds(2))}" }
            throw HttpServerErrorException(response.statusCode(), "Error while submitting work to master!")
        }
    }
}