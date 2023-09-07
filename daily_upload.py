from app import upload_files, organize_files, directory, s3_client


def upload_directory():
    client = s3_client
    files = organize_files(directory)
    upload_files(files, client, force_upload=True)


if __name__ == '__main__':
    print(f"Uploading data from {directory}")
    upload_directory()
