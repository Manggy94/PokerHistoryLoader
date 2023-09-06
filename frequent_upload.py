from app import s3_client, upload_today_files


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    upload_today_files(s3_client)

