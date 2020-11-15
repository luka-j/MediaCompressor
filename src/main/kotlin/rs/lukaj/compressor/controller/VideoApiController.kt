package rs.lukaj.compressor.controller

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.*
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.model.Video
import rs.lukaj.compressor.service.VideoService
import rs.lukaj.compressor.util.EntityNotFound
import rs.lukaj.compressor.util.replaceFileExtension
import java.time.LocalDateTime
import java.util.*
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

const val FILE_SIZE_HEADER = "File-Size"

@Controller
@RequestMapping("/video")
class VideoApiController(
        @Autowired val service: VideoService,
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

    @GetMapping("/{videoId}", produces = [MediaType.APPLICATION_OCTET_STREAM_VALUE])
    fun downloadVideo(@PathVariable("videoId") videoId: UUID, response: HttpServletResponse) {
        logger.info { "Received request to download file $videoId" }
        val video = service.getVideo(videoId).orElseThrow {EntityNotFound("Video with id $videoId doesn't exist!")}
        val file = service.downloadVideo(video)
        //this is the only way I found to send raw response
        response.setHeader("Content-Disposition", "attachment; " +
                "filename=\"${video.name.replaceFileExtension(properties.getVideoExtension())}\"")
        response.setHeader("Content-Length", file.length().toString())
        file.inputStream().copyTo(response.outputStream, 131072)
        response.status = 200
    }

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

    //this is not exactly safe - anyone could snoop for emails. but then again, this is not the next facebook
    //and I have tiny disk space, so old videos will probably get deleted not long after, returning nothing here
    @GetMapping("/status")
    fun getVideosStatus(@RequestParam("user") email: String) : ResponseEntity<List<Video>> {
        return ResponseEntity.ok(service.getVideosForUser(email))
    }
}