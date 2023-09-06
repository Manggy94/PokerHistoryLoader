from app import upload_files, organize_files, directory, s3_client

def upload_directory():
    client = s3_client
    files = organize_files(directory)
    upload_files(files, client)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print(directory)
    upload_directory()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
