package rs.lukaj.compressor.dao

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import rs.lukaj.compressor.model.Worker
import rs.lukaj.compressor.model.WorkerRepository
import rs.lukaj.compressor.model.WorkerStatus
import java.time.LocalDateTime
import java.time.ZoneOffset

@Service
class WorkerDao(
        @Autowired private val repository: WorkerRepository
) {
    fun getOrCreateWorker(host: String) : Worker {
        return repository.findByHostEquals(host).orElseGet {
            val worker = Worker(null, host, WorkerStatus.DOWN, 0,
                    LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC), 0)
            repository.save(worker)
        }
    }

    fun setWorkerQueueSize(worker: Worker, queueSize: Int) : Worker {
        worker.queueSize = queueSize
        return repository.save(worker)
    }

    fun setWorkerStatus(worker: Worker, status: WorkerStatus) {
        if(worker.status == WorkerStatus.UP) worker.lastAliveTime = LocalDateTime.now()
        if(status == WorkerStatus.DOWN) worker.downPings++
        else worker.downPings = 0
        worker.status = status
        repository.save(worker)
    }
}