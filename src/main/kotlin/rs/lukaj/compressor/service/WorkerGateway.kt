package rs.lukaj.compressor.service

import mu.KotlinLogging
import org.apache.http.HttpException
import org.apache.http.client.config.RequestConfig
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.FileEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler
import org.apache.http.impl.client.HttpClientBuilder
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
import rs.lukaj.compressor.dao.VideoDao
import rs.lukaj.compressor.dto.QueueSizeResponse
import rs.lukaj.compressor.util.addTrailingSlash
import java.time.Duration
import java.util.*


@Service
class WorkerGateway(
    @Autowired private val properties: EnvironmentProperties,
    @Autowired private val videoDao: VideoDao,
    @Autowired private val files : FileService
) {
    private val logger = KotlinLogging.logger {}
    private val client = WebClient.create()

    fun sendResultToMaster(originId: UUID, returnUrl: String) {
        val apacheClient = buildApacheClient(properties.getSubmitWorkToMasterRetryAttempts(),
                properties.getSubmitWorkToMasterTimeout())
        val request = HttpPost(returnUrl)
        request.addHeader(VIDEO_ID_HEADER, originId.toString())
        request.entity = FileEntity(files.getResultVideo(originId))
        apacheClient.execute(request) { response ->
            if (response.statusLine.statusCode != 200) {
                logger.error {
                    "Error occurred while submitting work to master! Got code ${response.statusLine.statusCode} " +
                            "and body: " + String(response.entity.content.readNBytes(1024))
                }
                throw HttpException("Error while submitting work to master!")
            }
        }
        apacheClient.close()
    }

    fun sendWorkToWorker(host: String, videoId: UUID) {
        val apacheClient = buildApacheClient(properties.getSendWorkToWorkerTimeoutRetryAttempts(),
                properties.getSendWorkToWorkerTimeout())
        val request = HttpPost(host.addTrailingSlash() + "worker/")
        request.addHeader(MASTER_KEY_HEADER, properties.getMyMasterKey())
        request.addHeader(FILE_NAME_HEADER, videoDao.getVideo(videoId).orElseThrow().name)
        request.addHeader(VIDEO_ID_HEADER, videoId.toString())
        request.addHeader(RETURN_URL_HEADER, properties.getHostUrl()!!.addTrailingSlash() + "worker/accept")
        request.entity = FileEntity(files.getQueueVideo(videoId))
        apacheClient.execute(request) { response ->
            if (response.statusLine.statusCode != 202) {
                logger.error {
                    "Error occurred while sending work to worker! Got code ${response.statusLine.statusCode} " +
                            "and body: " + String(response.entity.content.readNBytes(1024))
                }
                throw HttpException("Error while sending work to worker!")
            }
        }
        apacheClient.close()
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

    fun isVideoInQueue(host: String, videoId: UUID) : Boolean {
        return client.get()
                .uri {builder -> builder.host(host).path("/worker/queue/exists").queryParam("videoId", videoId).build() }
                .header(MASTER_KEY_HEADER, properties.getMyMasterKey())
                .exchange()
                .blockOptional(Duration.ofSeconds(properties.getQueueIntegrityCheckTimeout()))
                .orElseThrow {
                    logger.error { "Error occurred while checking queue integrity of worker $host" }
                    HttpServerErrorException(HttpStatus.INTERNAL_SERVER_ERROR)
                }
                .statusCode() == HttpStatus.OK
    }


    private fun buildApacheClient(retries: Int, timeout: Int) : CloseableHttpClient {
        return HttpClientBuilder.create()
                .setRetryHandler(DefaultHttpRequestRetryHandler(retries, true))
                .setDefaultRequestConfig(RequestConfig.custom().setSocketTimeout(timeout)
                        .setConnectTimeout(timeout).build())
                .build()
    }
}