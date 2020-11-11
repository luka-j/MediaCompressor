package rs.lukaj.compressor.scheduled

import org.springframework.stereotype.Service

@Service
class ZombieVideoCleanup {
    //todo remove files for ERROR videos, if originated locally, after some time (only if free space is low?)
    //todo remove files for transitive states (UPLOADED (??) / PROCESSED) after some time has passed; introduce new IN_QUEUE state?
    //todo reassign jobs executing remotely, if they've been processing for too long (and mark worker as DOWN, to prevent reassignment?)
    //todo remove files for other states, if it has not been touched for a long time and space is low (?)
}