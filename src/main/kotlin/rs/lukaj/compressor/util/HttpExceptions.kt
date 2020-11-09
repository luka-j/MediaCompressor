package rs.lukaj.compressor.util

import org.springframework.http.HttpStatus

abstract class HttpException(val returnCode: HttpStatus, val msg: String) : Exception(msg) {
    fun toResponse() = ErrorResponse(this.javaClass.simpleName, msg)
}

class QueueFullException : HttpException(HttpStatus.SERVICE_UNAVAILABLE, "Processing queue is currently full. Try again later.")
class NotEnoughSpaceException : HttpException(HttpStatus.SERVICE_UNAVAILABLE, "Processing queue is currently full: not enough space. Try again later.")
class InternalServerError : HttpException(HttpStatus.INTERNAL_SERVER_ERROR, "Something went wrong. Try again later.")


data class ErrorResponse(val code: String, val message: String)