package rs.lukaj.compressor.model

import org.hibernate.annotations.GenericGenerator
import java.time.LocalDateTime
import java.util.*
import javax.persistence.*

@Entity
@Table(name="workers")
class Worker(
        @Id @GeneratedValue(generator = "uuid4")
        @GenericGenerator(name = "uuid4", strategy = "org.hibernate.id.UUIDGenerator")
        @Column(name = "id", columnDefinition = "uuid", nullable = false) var id: UUID?,

        @Column(nullable=false, unique = true) var host: String,
        @Enumerated(EnumType.STRING) var status: WorkerStatus,
        @Column(nullable=false) var queueSize: Int,
        @Column(nullable=false) var lastAliveTime: LocalDateTime,
        @Column(nullable = false) var downPings: Int
) : AuditModel(null, null)

enum class WorkerStatus {
    UP,
    DOWN
}