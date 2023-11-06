import datetime
from loader import S3Uploader

if __name__ == '__main__':
    uploader = S3Uploader()
    year = datetime.date.today().year
    date = datetime.date(year, 8, 1)
    uploader.upload_files_since(date=date, force_upload=True)
