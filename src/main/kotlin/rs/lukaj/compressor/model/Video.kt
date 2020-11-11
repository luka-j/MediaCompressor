package rs.lukaj.compressor.model

import org.hibernate.annotations.GenericGenerator
import java.util.*
import javax.persistence.*

@Entity
@Table(name = "videos")
class Video(
        @Id @GeneratedValue(generator = "uuid4")
        @GenericGenerator(name = "uuid4", strategy = "org.hibernate.id.UUIDGenerator")
        @Column(name = "id", columnDefinition = "uuid", nullable = false) var id: UUID?,

        @Column(nullable=false, columnDefinition = "text") var name: String,
        @Column(nullable=false, columnDefinition = "text") var email: String,
        @Column(nullable=false) var originalSize: Long,
        @Column(nullable=false) var compressedSize: Long,
        @Column(name = "transcoding_progress", nullable=false) var transcodingProgress: Int,
        @Column(name = "transcoding_speed", nullable = false) var transcodingSpeed: Float,
        @Enumerated(EnumType.STRING) @Column(nullable=false) var status: VideoStatus,
        @Column(nullable=false) var node: String,
        @Column(nullable=false) var origin: String
        )
    : AuditModel(null, null)

enum class VideoStatus {
    UPLOADING,
    UPLOADED,
    IN_QUEUE,
    PROCESSING,
    PROCESSED,
    EMAIL_PENDING,
    READY,
    DOWNLOADED,
    DELETED,
    DELETED_WITHOUT_DOWNLOADING,
    ERROR
}