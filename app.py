from fastapi import FastAPI, HTTPException
import os
import boto3
import re
from dotenv import load_dotenv
import datetime

load_dotenv()

app = FastAPI()
directory = r'C:/Users/mangg/AppData/Local/PokerTracker 4/Processed/Winamax'
s3_client = boto3.client('s3',
                         region_name=os.environ.get("DO_REGION"),
                         endpoint_url=os.environ.get("DO_ENDPOINT"),
                         aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                         aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY")
                         )


@app.get("/")
def read_root():
    return {"Hello": "World"}

def get_files(directory_path: str):
    pattern = r"(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})_(?P<tournament_name>.+)\((?P<tournament_id>\d+)\)_real_holdem_no-limit(?:_summary)?(?:_1)?\.txt"
    files = []
    for dirpath, _, filenames in os.walk(directory_path):
        for file in filenames:
            if re.match(pattern, file):
                files.append(os.path.join(dirpath, file))
    return files


def extract_file_info(file_name):
    pattern = r"(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})_(?P<tournament_name>.+)\((?P<tournament_id>\d+)\)_real_holdem_no-limit(?:_summary)?(?:_1)?\.txt"
    match = re.match(pattern, file_name)

    if not match:
        return None

    if "_summary.txt" in file_name:
        file_type = "summary"
    else:
        file_type = "history"

    return {
        "year": match.group("year"),
        "month": match.group("month"),
        "day": match.group("day"),
        "tournament_name": match.group("tournament_name"),
        "tournament_id": match.group("tournament_id"),
        "file_type": file_type,
        "date": datetime.date(int(match.group("year")), int(match.group("month")), int(match.group("day")))
    }


def get_files_info(directory_path: str):
    files_info = []
    for dirpath, _, filenames in os.walk(directory_path):
        for file in filenames:
            file_info = extract_file_info(file)
            if file_info:
                files_info.append(file_info)
    return files_info


def create_path(file_info: dict):
    if file_info["file_type"] == "summary":
        path = f"data/summaries/{file_info['year']}/{file_info['month']}/{file_info['day']}/{file_info['tournament_id']}_{file_info['tournament_name']}_summary.txt"
    else:
        path = f"data/histories/raw/{file_info['year']}/{file_info['month']}/{file_info['day']}/{file_info['tournament_id']}_{file_info['tournament_name']}_history.txt"
    return path


def organize_files(directory_path: str):
    files_info = get_files_info(directory_path)
    paths = [create_path(file_info) for file_info in files_info]
    files = get_files(directory)
    organized_files = [
        {
            "file_info": f_info,
            "s3_path": p,
            "local_path": f}
        for f_info, p, f in zip(files_info, paths, files)]
    return organized_files


def organize_today_files(directory_path: str):
    files_info = get_files_info(directory_path)
    paths = [create_path(file_info) for file_info in files_info]
    files = get_files(directory_path)
    organized_files = [
        {"file_info": f_info,
         "s3_path": p,
         "local_path": f}
        for f_info, p, f in zip(files_info, paths, files)
        if f_info["date"] == datetime.date.today()]
    return organized_files


def upload_file(file: dict, client):
    bucket_name = 'manggy-poker'
    client.upload_file(file["local_path"], bucket_name, file["s3_path"])


def upload_files(files: list, client):
    for file in files:
        if not check_file_exists(client, file):
            upload_file(file, client)


def download_file(client, bucket_name: str, key: str, local_path: str):
    client.download_file(bucket_name, key, local_path)


@app.post("/upload-files/")
def upload_files_to_s3():
    files = organize_files(directory)
    upload_files(files, s3_client)
    return {"message": "Files uploaded successfully"}


def list_buckets(client):
    response = client.list_buckets()
    return response.get('Buckets')


def list_objects(client, bucket_name: str = "manggy-poker"):
    response = client.list_objects(Bucket=bucket_name)
    return response.get('Contents')


def get_object(client, bucket_name: str, key: str):
    response = client.get_object(Bucket=bucket_name, Key=key)
    return response


def check_file_exists(client, file: dict):
    try:
        bucket_name = 'manggy-poker'
        key = file.get("s3_path")
        get_object(client, bucket_name, key)
        return True
    except client.exceptions.NoSuchKey:
        return False


def upload_today_files(client):
    files = organize_today_files(directory)
    for file in files:
        upload_file(file, client)


@app.post("/upload-today-files/")
def upload_today_files_to_s3():
    upload_today_files(s3_client)
    return {"message": "Files uploaded successfully"}
