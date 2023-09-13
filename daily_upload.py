import datetime
from loader import S3Uploader

if __name__ == '__main__':
    uploader = S3Uploader()
    year = datetime.date.today().year
    uploader.upload_files_since(datetime.date(year=year, month=1, day=1), force_upload=True)
