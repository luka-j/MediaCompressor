package rs.lukaj.compressor.controller

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.dto.QueueSizeResponse
import rs.lukaj.compressor.service.VideoCrudService
import rs.lukaj.compressor.util.MasterNotAuthorized
import rs.lukaj.compressor.util.WorkerModeNotAllowed
import java.util.*
import javax.servlet.http.HttpServletRequest

const val MASTER_KEY_HEADER = "Master-Key"
const val FILE_NAME_HEADER = "File-Name"
const val VIDEO_ID_HEADER = "Video-Id"
const val RETURN_URL_HEADER = "Return-Url"

@Controller
@RequestMapping("/worker")
class WorkerActionsController(
        @Autowired val service: VideoCrudService,
        @Autowired val properties: EnvironmentProperties
) {
    private val logger = KotlinLogging.logger {}

    private fun ensureMasterAuthorized(key: String, request: HttpServletRequest) {
        if(!properties.isWorkerModeEnabled()) {
            logger.warn { "Someone attempted to use worker endpoint ${request.requestURI} from ${request.remoteAddr}" }
            throw WorkerModeNotAllowed()
        }
        val keys = properties.getAllowedMasterHosts()
        if(keys.size == 1 && keys[0] == "*") return
        if(!keys.contains(key)) {
            logger.warn { "Someone attempted to use master key $key for endpoint ${request.requestURI} from ${request.remoteAddr}" }
            throw MasterNotAuthorized()
        }
    }

    @GetMapping("/queue/size")
    fun getQueueSize(@RequestHeader(MASTER_KEY_HEADER) key: String, request: HttpServletRequest) : ResponseEntity<QueueSizeResponse> {
        ensureMasterAuthorized(key, request)
        return ResponseEntity.ok(QueueSizeResponse(service.getQueueSize()))
    }

    @GetMapping("/ping")
    fun ping(@RequestHeader(MASTER_KEY_HEADER) key: String, request: HttpServletRequest) :ResponseEntity<String> {
        ensureMasterAuthorized(key, request)
        return ResponseEntity.ok("pong")
    }

    @PostMapping("/")
    fun addVideoToQueue(@RequestHeader(MASTER_KEY_HEADER) key: String,
                        @RequestHeader(FILE_NAME_HEADER) filename: String,
                        @RequestHeader(VIDEO_ID_HEADER) videoId: UUID,
                        @RequestHeader(RETURN_URL_HEADER) returnUrl: String,
                        request: HttpServletRequest) : ResponseEntity<Any> {
        ensureMasterAuthorized(key, request)

        service.addVideoFromMaster(request.inputStream, videoId, filename, request.contentLengthLong, key, returnUrl)
        return ResponseEntity.status(HttpStatus.ACCEPTED).build<Any>()
    }

    @PostMapping("/accept")
    fun acceptResult(@RequestHeader(MASTER_KEY_HEADER) key: String,
                     @RequestHeader(VIDEO_ID_HEADER) videoId: UUID,
                     request: HttpServletRequest) : ResponseEntity<Any> {
        ensureMasterAuthorized(key, request)

        service.acceptProcessedVideo(request.inputStream, videoId)
        return ResponseEntity.ok().build<Any>()
    }
}