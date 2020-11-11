package rs.lukaj.compressor.service

import com.sendgrid.Method
import com.sendgrid.Request
import com.sendgrid.Response
import com.sendgrid.SendGrid
import com.sendgrid.helpers.mail.Mail
import com.sendgrid.helpers.mail.objects.Content
import com.sendgrid.helpers.mail.objects.Email
import mu.KotlinLogging
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rs.lukaj.compressor.configuration.EnvironmentProperties
import java.io.IOException

@Service
class SendGridGateway(@Autowired private val properties : EnvironmentProperties) {
    private val logger = KotlinLogging.logger {}

    fun sendMail(subject: String, body: String, isHtml : Boolean, to: Email,
                 from: Email = Email(properties.getMailSendingAddress())) {
        val content = Content(if(isHtml) "text/html" else "text/plain", body)
        val mail = Mail(from, subject, to, content)

        val apiKey = properties.getSendgridApiKey()
        if(apiKey == null) {
            logger.info { "SendGrid apiKey property is null; not sending email" }
            return
        }
        val sg = SendGrid(apiKey)
        val request = Request()
        try {
            request.method = Method.POST
            request.endpoint = "mail/send"
            request.body = mail.build()
            val response: Response = sg.api(request)
            logger.info {"Sent email to ${to.email}! Sendgrid status ${response.statusCode}" }
        } catch (ex: IOException) {
            logger.error(ex) {"Error while sending email!"}
            throw ex
        }
    }
}