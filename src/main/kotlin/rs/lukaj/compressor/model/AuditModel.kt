package rs.lukaj.compressor.model

import org.hibernate.annotations.CreationTimestamp
import org.hibernate.annotations.UpdateTimestamp
import java.time.LocalDateTime
import javax.persistence.Column
import javax.persistence.MappedSuperclass

@MappedSuperclass
open class AuditModel(
        @CreationTimestamp @Column(name = "created_at") var createdAt: LocalDateTime?,
        @UpdateTimestamp @Column(name = "updated_at") var updatedAt: LocalDateTime?
)