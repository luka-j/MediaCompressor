package rs.lukaj.compressor.dao

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rs.lukaj.compressor.model.Worker
import rs.lukaj.compressor.model.WorkerRepository
import rs.lukaj.compressor.model.WorkerStatus
import java.time.LocalDateTime

@Service
class WorkerDao(
        @Autowired private val repository: WorkerRepository
) {
    fun getOrCreateWorker(host: String) : Worker {
        return repository.findByHostEquals(host).orElseGet {
            val worker = Worker(null, host, WorkerStatus.DOWN, 0, LocalDateTime.MIN)
            repository.save(worker)
        }
    }
}