from loader import S3Uploader

if __name__ == '__main__':
    uploader = S3Uploader()
    uploader.upload_today_files(force_upload=True)

