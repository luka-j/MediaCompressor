package rs.lukaj.compressor

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.data.jpa.repository.config.EnableJpaAuditing
import org.springframework.data.jpa.repository.config.EnableJpaRepositories
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
@EnableJpaRepositories
@EnableJpaAuditing
@EnableScheduling
class CompressorApplication

fun main(args: Array<String>) {
    runApplication<CompressorApplication>(*args)
}
