package rs.lukaj.compressor.scheduled

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import rs.lukaj.compressor.configuration.EnvironmentProperties
import rs.lukaj.compressor.dao.WorkerDao
import rs.lukaj.compressor.model.WorkerStatus
import rs.lukaj.compressor.service.WorkerGateway

@Service
class WorkerDaemon(
        @Autowired private val properties: EnvironmentProperties,
        @Autowired private val dao : WorkerDao,
        @Autowired private val workerService: WorkerGateway
) {
    @Scheduled(fixedDelay = 60_000)
    fun pingWorkers() {
        val workerHosts = properties.getAvailableWorkers()
        for(host in workerHosts) {
            val worker = dao.getOrCreateWorker(host)
            val isUp = try {
                workerService.ping(host)
            } catch (e: Exception) {
                false
            }
            if((worker.status == WorkerStatus.UP) == isUp) continue //status has not changed

            if(!isUp) {
                if(worker.queueSize <= 0) continue
                //todo worker has gone down, reassign work
            } else {
                //todo we have new worker up, assign something to it
            }
        }
    }
}