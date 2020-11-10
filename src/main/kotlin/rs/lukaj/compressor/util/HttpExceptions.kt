package rs.lukaj.compressor.util

import org.springframework.http.HttpStatus

abstract class HttpException(val returnCode: HttpStatus, val msg: String) : Exception(msg) {
    fun toResponse() = ErrorResponse(this.javaClass.simpleName, msg)
}

class QueueFull : HttpException(HttpStatus.SERVICE_UNAVAILABLE, "Processing queue is currently full. Try again later.")
class NotEnoughSpace : HttpException(HttpStatus.SERVICE_UNAVAILABLE, "Processing queue is currently full: not enough space. Try again later.")
class InternalServerError : HttpException(HttpStatus.INTERNAL_SERVER_ERROR, "Something went wrong. Try again later.")
class EntityNotFound(msg: String) : HttpException(HttpStatus.NOT_FOUND, msg)
class InvalidStatus(msg: String) : HttpException(HttpStatus.CONFLICT, msg)
class WorkerModeNotAllowed : HttpException(HttpStatus.FORBIDDEN, "Contact server administrator (or they'll contact you)")
class MasterNotAuthorized : HttpException(HttpStatus.UNAUTHORIZED, "Contact server administrator (or they'll contact you)")


data class ErrorResponse(val code: String, val message: String)