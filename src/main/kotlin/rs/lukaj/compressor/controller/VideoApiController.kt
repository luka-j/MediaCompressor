package rs.lukaj.compressor.controller

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.*
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.service.VideoCrudService
import java.io.File
import java.time.LocalDateTime
import java.util.*
import javax.servlet.http.HttpServletRequest

const val FILE_SIZE_HEADER = "File-Size"

@Controller
@RequestMapping("/video")
class VideoApiController(
        @Autowired val service: VideoCrudService,
        @Autowired val properties: EnvironmentProperties
) {

    private val logger = KotlinLogging.logger {}
    private var lastQueueFullResponse = Pair(LocalDateTime.MIN, HttpStatus.OK)

    @PostMapping("/", consumes = ["*/*"])
    fun uploadVideo(@RequestHeader("User-Email", required = true) email: String,
                    @RequestHeader("File-Name") filename: String,
                    request: HttpServletRequest) : ResponseEntity<Any> {
        logger.info { "Received request to add file $filename with size ${request.contentLengthLong} to queue for $email" }
        service.addToQueue(request.inputStream, filename, request.contentLengthLong, email)
        return ResponseEntity.ok().build()
    }

    @GetMapping("/{videoId}")
    fun downloadVideo(@PathVariable("videoId") videoId: UUID) : ResponseEntity<File> {
        logger.info { "Received request to download file $videoId" }
        return ResponseEntity.ok(service.getVideo(videoId))
    }

    //todo use this on frontend to alert user before uploading
    @GetMapping("/queue/status")
    fun isQueueFull(@RequestHeader(MASTER_KEY_HEADER, defaultValue = "") key: String,
                    @RequestHeader(FILE_SIZE_HEADER, defaultValue = (400 * 1024 * 1024).toString()) size: Long) : ResponseEntity<Any> {
        if(key == properties.getMyMasterKey()) return ResponseEntity.ok().build()

        val now = LocalDateTime.now()
        if(lastQueueFullResponse.first.plusSeconds(properties.getQueueFullResponseCachingTime()).isAfter(now)) {
            return ResponseEntity.status(lastQueueFullResponse.second).build()
        }

        return if(service.isQueueFull(key, size)) {
            lastQueueFullResponse = Pair(now, HttpStatus.SERVICE_UNAVAILABLE)
            ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build()
        } else {
            lastQueueFullResponse = Pair(now, HttpStatus.OK)
            ResponseEntity.ok().build()
        }
    }
}