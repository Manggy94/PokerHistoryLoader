import datetime
import threading

import boto3
import os
import re
import dotenv
from functools import cached_property

dotenv.load_dotenv()


def dates_since(date: datetime.date) -> list:
    """
    Get all the dates since a date
    :return: A list of all the dates since a date
    """
    dates = []
    start_date = date
    today = datetime.date.today()
    date = start_date
    while date <= today:
        dates.append(date)
        date += datetime.timedelta(days=1)
    return dates


class S3Uploader:
    """
    A class to upload Hand history files to S3 bucket
    """

    def __init__(self,
                 directory: str = r'C:/Users/mangg/AppData/Local/PokerTracker 4/Processed/Winamax',
                 bucket_name: str = "manggy-poker"):
        """
        Constructor
        :param directory: The local directory where the files are stored
        :param bucket_name: The name of the storage bucket where files will be uploaded
        """
        self.s3 = boto3.resource(
            "s3",
            region_name=os.environ.get("DO_REGION"),
            endpoint_url=os.environ.get("DO_ENDPOINT"),
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
        )
        self.bucket = self.s3.Bucket(bucket_name)
        self.directory = directory
        self.pattern = r"(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})_(?P<tournament_name>.+)\((?P<tournament_id>\d+)\)_real_holdem_no-limit(?:_summary)?(?:_1)?\.txt"

    def get_local_files(self) -> list:
        """
        Get all the files in the directory
        :return: A list of all the files in the directory
        """
        files = []
        for dirpath, _, filenames in os.walk(self.directory):
            for file in filenames:
                if re.match(self.pattern, file):
                    files.append(os.path.join(dirpath, file))
        return files

    def extract_file_info(self, file_name: str) -> dict or None:
        """
        Extract information from the file name
        :param file_name: The name of the file
        :return: A dict with the information extracted from the file name( year, month, day, tournament_name,
        tournament_id, file_type)
        """
        match = re.match(self.pattern, file_name)

        if not match:
            return None

        if "_summary.txt" in file_name:
            file_type = "summary"
        else:
            file_type = "history"

        return {
            "year": int(match.group("year")),
            "month": int(match.group("month")),
            "day": int(match.group("day")),
            "tournament_name": match.group("tournament_name"),
            "tournament_id": match.group("tournament_id"),
            "file_type": file_type,
            "date": datetime.date(int(match.group("year")), int(match.group("month")), int(match.group("day")))
        }

    def get_files_info(self) -> list:
        """
        Get all the files info in the directory
        :return: A list of all the files info in the directory
        """
        files_info = []
        for dirpath, _, filenames in os.walk(self.directory):
            for file in filenames:
                file_info = self.extract_file_info(file)
                if file_info:
                    files_info.append(file_info)
        return files_info

    @staticmethod
    def create_path(file_info: dict) -> str:
        """
        Create the path in for the object in the bucket
        :param file_info: The file info
        :return: The path of the object in the bucket
        """
        if file_info["file_type"] == "summary":
            path = f"data/summaries/{file_info['year']:04}/{file_info['month']:02}/{file_info['day']:02}/" \
                   f"{file_info['tournament_id']}_{file_info['tournament_name']}_summary.txt"
        else:
            path = f"data/histories/raw/{file_info['year']:04}/{file_info['month']:02}/{file_info['day']:02}/" \
                   f"{file_info['tournament_id']}_{file_info['tournament_name']}_history.txt"
        return path

    @cached_property
    def organized_files(self) -> list:
        """
        Organize the files in the directory
        :return: A list of dict with the file info, the s3 path and the local path for each file
        """
        files_info = self.get_files_info()
        paths = [self.create_path(file_info) for file_info in files_info]
        files = self.get_local_files()
        organized_files = [
            {
                "file_info": f_info,
                "s3_path": p,
                "local_path": f}
            for f_info, p, f in zip(files_info, paths, files)
        ]
        return organized_files

    def organize_by_year(self, year: int):
        """
        Organize the files in the directory
        :param year: The year to filter
        :return: The same list as organized_files function, filtered by year
        """
        return [file for file in self.organized_files if file["file_info"]["year"] == year]

    def organize_by_month_of_year(self, month: int, year: int):
        """
        Organize the files in the directory
        :param month: The month to filter
        :param year: The year to filter
        :return: The same list as organized_files function, filtered by month and year
        """
        return [file for file in self.organize_by_year(year) if int(file["file_info"]["month"]) == int(str(month))]

    def organize_by_date(self, date: datetime.date):
        """
        Organize the files in the directory
        :param date: The date to filter
        :return: The same list as organized_files function, filtered by date
        """
        return [file for file in self.organized_files if file["file_info"]["date"] == date]

    def upload_file(self, file: dict):
        """
        Upload a file to the bucket with information from organized_files dict format
        :param file: The dict of the file to upload
        """
        self.bucket.upload_file(
            Filename=file["local_path"],
            Key=file["s3_path"],
            ExtraArgs={
                'ACL': 'public-read',
                'Metadata': {
                    'year': f'{file["file_info"]["year"]:04}',
                    'month': f'{file["file_info"]["month"]:02}',
                    'day': f'{file["file_info"]["day"]:02}',
                    'tournament_id': file["file_info"]["tournament_id"],
                    'file_type': file["file_info"]["file_type"],
                    'date': file["file_info"]["date"].isoformat()
                }
            })

    def check_file_exists(self, file: dict):
        """
        Check if a file exists in the bucket
        :param file: The dict of the file to check
        :return: True if the file exists, False otherwise
        """
        return bool(list(self.bucket.objects.filter(Prefix=file["s3_path"])))

    def upload_files(self, force_upload: bool = False):
        """
        Upload files to the bucket
        :param force_upload: If True, upload all the files, even if they already exist in the bucket
        """
        threads = []
        for file in self.organized_files:
            if force_upload or not self.check_file_exists(file):
                thread = threading.Thread(target=self.upload_file, args=(file,))
                thread.start()
                threads.append(thread)
        for thread in threads:
            thread.join()

    def upload_today_files(self, force_upload: bool = True):
        """
        Upload files of the day to the bucket
        :param force_upload: If True, upload all the files, even if they already exist in the bucket
        """
        threads = []
        for file in self.organize_by_date(datetime.date.today()):
            if force_upload or not self.check_file_exists(file):
                thread = threading.Thread(target=self.upload_file, args=(file,))
                thread.start()
                threads.append(thread)
        for thread in threads:
            thread.join()

    def upload_files_since(self, date: datetime.date, force_upload: bool = True):
        """
        Upload files since a date
        :param date:
        :param force_upload:
        :return:
        """
        for date in dates_since(date):
            for file in self.organize_by_date(date):
                if force_upload or not self.check_file_exists(file):
                    self.upload_file(file)
