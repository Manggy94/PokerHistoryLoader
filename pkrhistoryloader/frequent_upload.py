from loader import S3Uploader
from watchdog.observers import Observer

if __name__ == '__main__':
    uploader = S3Uploader()
    observer = Observer()
    observer.schedule(uploader, uploader.processing_directory, recursive=True)
    observer.schedule(uploader, uploader.directory, recursive=True)
    observer.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        observer.stop()
