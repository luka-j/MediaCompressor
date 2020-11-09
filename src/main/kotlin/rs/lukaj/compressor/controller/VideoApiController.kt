package rs.lukaj.compressor.controller

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.*
import rs.lukaj.compressor.service.VideoCrudService
import java.io.File
import java.util.*
import javax.servlet.http.HttpServletRequest

@Controller("/video")
@RequestMapping("/video")
class VideoApiController(@Autowired val service: VideoCrudService) {

    private val logger = KotlinLogging.logger {}

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
}