package rs.lukaj.compressor.model

import org.hibernate.annotations.GenericGenerator
import java.util.*
import javax.persistence.*

@Entity
@Table(name = "videos")
class Video(
        @Id @GeneratedValue(generator = "uuid4")
        @GenericGenerator(name = "uuid4", strategy = "org.hibernate.id.UUIDGenerator")
        @Column(name = "id", columnDefinition = "uuid") var id: UUID?,

        var name: String,
        var email: String,
        var originalSize: Long,
        var compressedSize: Long,
        @Column(name = "transcoding_progress") var transcodingProgress: Int,
        @Column(name = "transcoding_speed") var transcodingSpeed: Float,
        @Enumerated(EnumType.STRING) var status: VideoStatus
        )
    : AuditModel(null, null)

enum class VideoStatus {
    UPLOADING,
    UPLOADED,
    PROCESSING,
    PROCESSED,
    EMAIL_PENDING,
    READY,
    DOWNLOADED,
    DELETED,
    DELETED_WITHOUT_DOWNLOADING
}