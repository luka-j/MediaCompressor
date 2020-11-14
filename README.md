# MediaCompressor
### transcodes videos to HEVC / AAC (and applies a few optimizations)

This project is primarily aimed at reducing video file size, at minimal expense of quality. Resolution is never reduced. For some parameters, take a look at configutaion.EnvironmentProperties class.

Uses a distributed work queue to split work between multiple machines (visible on public internet). Notifies user via email when all work is done.

## Requirements
- Java 11
- ffmpeg with libx265 encoder installed
- PostgreSQL database up and running
- SendGrid account (optional)

//todo write a proper readme