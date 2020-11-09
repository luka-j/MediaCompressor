package rs.lukaj.compressor.controller

import org.springframework.stereotype.Controller
import org.springframework.web.bind.annotation.GetMapping

@Controller
class ViewController {
    @GetMapping
    fun videoUploadForm() : String {
        return "uploadVideo"
    }
}