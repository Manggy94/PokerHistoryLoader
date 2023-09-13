from loader import S3Uploader

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    uploader = S3Uploader()
    uploader.upload_today_files(force_upload=True)

