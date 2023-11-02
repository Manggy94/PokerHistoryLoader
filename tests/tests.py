from unittest import TestCase
from pkrhistoryloader.loader import S3Uploader
import datetime

class TestS3Uploader(TestCase):
    """
    A class to test the S3Uploader class
    """

    def setUp(self):
        """
        Set up the test
        """
        self.s3_uploader = S3Uploader()

    def test_s3_connection(self):
        """
        Test the s3 connection
        """
        print(self.s3_uploader.s3)
        self.assertIsNotNone(self.s3_uploader.s3)
        print(self.s3_uploader.bucket)
        self.assertIsNotNone(self.s3_uploader.bucket)

    def test_get_local_files(self):
        """
        Test the get_local_files method
        """
        local_files = self.s3_uploader.get_local_files()
        self.assertIsInstance(local_files, list)
        for file in local_files:
            self.assertIsInstance(file, str)
            self.assertIn(".txt", file)
            self.assertIn(self.s3_uploader.directory, file)

    def test_get_files_info(self):
        """
        Test the get_files_info method
        """
        files_info = self.s3_uploader.get_files_info()
        self.assertIsInstance(files_info, list)
        for file_info in files_info:
            self.assertIsInstance(file_info, dict)
            self.assertIn("year", file_info)
            self.assertIsInstance(file_info["year"], int)
            self.assertIn("month", file_info)
            self.assertIsInstance(file_info["month"], int)
            self.assertIn("day", file_info)
            self.assertIsInstance(file_info["day"], int)
            self.assertIn("tournament_id", file_info)
            self.assertIsInstance(file_info["tournament_id"], str)
            self.assertIn("file_type", file_info)
            self.assertIsInstance(file_info["file_type"], str)
            self.assertIn(file_info.get("file_type"), ["summary", "history"])
            self.assertIn("date", file_info)
            self.assertIsInstance(file_info["date"], datetime.date)

    def test_create_path(self):
        file_info = {
            "year": 2020,
            "month": 12,
            "day": 31,
            "tournament_id": "123456789",
            "tournament_name": "Test",
            "file_type": "summary",
            "date": datetime.date(2020, 12, 31)
        }
        file_info2 = {
            "year": 2024,
            "month": 4,
            "day": 31,
            "tournament_id": "987654321",
            "tournament_name": "Test",
            "file_type": "history",
            "date": datetime.date(2020, 12, 31)
        }
        path = self.s3_uploader.create_path(file_info)
        self.assertIsInstance(path, str)
        self.assertEqual(path, "data/summaries/2020/12/31/123456789_Test_summary.txt")
        path2 = self.s3_uploader.create_path(file_info2)
        self.assertIsInstance(path2, str)
        self.assertNotEqual(path, path2)
        self.assertEqual(path2, "data/histories/raw/2024/04/31/987654321_Test_history.txt")

    def test_organized_files(self):
        """
        Test the organized_files property
        """
        organized_files = self.s3_uploader.organized_files
        self.assertIsInstance(organized_files, list)
        for file in organized_files:
            self.assertIsInstance(file, dict)
            self.assertIn("file_info", file)
            self.assertIsInstance(file["file_info"], dict)
            self.assertIn("s3_path", file)
            self.assertIsInstance(file["s3_path"], str)
            self.assertIn("local_path", file)
            self.assertIsInstance(file["local_path"], str)
            self.assertIn(".txt", file["local_path"])
            self.assertIn(self.s3_uploader.directory, file["local_path"])
            self.assertIn(file["file_info"]["file_type"], ["summary", "history"])
            self.assertIn(f'{file["file_info"]["year"]:04}', file["s3_path"])
            self.assertIn(f'{file["file_info"]["month"]:02}', file["s3_path"])
            self.assertIn(f'{file["file_info"]["day"]:02}', file["s3_path"])
            self.assertIn(f'{file["file_info"]["tournament_id"]}', file["s3_path"])
            self.assertIn(f'{file["file_info"]["tournament_name"]}', file["s3_path"])
            self.assertIn(f'{file["file_info"]["file_type"]}', file["s3_path"])
            self.assertIn(f'{file["file_info"]["year"]:04}', file["local_path"])
            self.assertIn(f'{file["file_info"]["month"]:02}', file["local_path"])
            self.assertIn(f'{file["file_info"]["day"]:02}', file["local_path"])
            self.assertIn(f'{file["file_info"]["tournament_id"]}', file["local_path"])
            self.assertIn(f'{file["file_info"]["tournament_name"]}', file["local_path"])

    def test_organize_by_year(self):
        """
        Test the organize_by_year method
        """
        organized_files = self.s3_uploader.organize_by_year(2020)
        self.assertIsInstance(organized_files, list)
        for file in organized_files:
            self.assertIsInstance(file, dict)
            self.assertEqual(file["file_info"]["year"], 2020)

    def test_organize_by_month_of_year(self):
        """
        Test the organize_by_month_of_year method
        """
        organized_files = self.s3_uploader.organize_by_month_of_year(12, 2020)
        self.assertIsInstance(organized_files, list)
        for file in organized_files:
            self.assertIsInstance(file, dict)
            self.assertEqual(file["file_info"]["year"], 2020)
            self.assertEqual(file["file_info"]["month"], 12)

    def test_organize_by_date(self):
        """
        Test the organize_by_date method
        """
        organized_files = self.s3_uploader.organize_by_date(datetime.date(2020, 12, 14))
        self.assertIsInstance(organized_files, list)
        for file in organized_files:
            self.assertIsInstance(file, dict)
            self.assertEqual(file["file_info"]["date"], datetime.date(2020, 12, 14))


