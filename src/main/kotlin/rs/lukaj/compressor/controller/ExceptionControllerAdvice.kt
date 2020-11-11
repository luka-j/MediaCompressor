package rs.lukaj.compressor.controller

import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ControllerAdvice
import org.springframework.web.bind.annotation.ExceptionHandler
import rs.lukaj.compressor.dao.VideoDao
import rs.lukaj.compressor.util.ErrorResponse
import rs.lukaj.compressor.util.HttpException
import javax.servlet.http.HttpServletRequest

@ControllerAdvice
class ExceptionControllerAdvice(
        @Autowired dao: VideoDao
) {
    val logger = KotlinLogging.logger {  }

    @ExceptionHandler(value = [HttpException::class])
    fun handleHttpException(exception: HttpException, request: HttpServletRequest) : ResponseEntity<ErrorResponse> {
        logger.warn { "Returning error response to request ${request.method} ${request.servletPath}: " +
                "${exception::class.simpleName} - ${exception.message}" }
        logger.debug(exception){ "Exception details" } //correlating this log with the previous one will (almost?) never be the problem

        return ResponseEntity.status(exception.returnCode).body(exception.toResponse())
    }

    @ExceptionHandler(value = [Exception::class])
    fun handleException(exception: Exception, request: HttpServletRequest) : ResponseEntity<ErrorResponse> {
        logger.error(exception) { "Unexpected error occurred" }

        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ErrorResponse("InternalServerError",
                "Something has gone wrong. Try again."))
    }
}