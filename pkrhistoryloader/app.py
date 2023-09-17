from loader import S3Uploader
import time
uploader = S3Uploader()

start = time.time()
for _ in range(2):
    uploader.upload_files(force_upload=True)
end = time.time()
duration = end - start
print(f"Temps d'exécution avec full Threading : {duration} secondes")

