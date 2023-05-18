"""
    cd backend && python upload_files.py
"""
from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

import os
import time
from datetime import datetime, timedelta
from csv import writer


def download_file_to_drive(filename):
    my_file = drive.CreateFile({'title': filename, 'parents': [{'id': logs_folder_id}]})

    my_file.SetContentFile(os.path.join('logs', filename))
    my_file.Upload()

    _ = my_file.InsertPermission({
        'type': 'anyone',
        'value': 'anyone',
        'role': 'reader'
    })

    return my_file['webContentLink']


def get_old_files(now):
    old_files = []
    prev_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    for filename in os.listdir('logs'):
        if prev_date in filename:
            old_files.append(filename)

    return old_files, prev_date


def write_to_table(arr):
    if not os.path.exists('../table.csv'):
        with open('../table.csv', 'a') as f_object:
            writer_object = writer(f_object)
            writer_object.writerow(['symbol', 'stream', 'date', 'link'])
            f_object.close()

    with open('../table.csv', 'a') as f_object:
        writer_object = writer(f_object)
        writer_object.writerow(arr)
        f_object.close()


def upload_pipeline(now):
    print('Start Pipeline')
    old_files, prev_date = get_old_files(now)
    print(old_files)
    from tqdm import tqdm
    for filename in tqdm(old_files):
        download_link = download_file_to_drive(filename)
        trade_symbol = filename.split('_')[0]
        stream_name = filename.split('_')[1]

        write_to_table([trade_symbol, stream_name, prev_date, download_link])

    # print('delete files')
    # # delete old logs
    # for filename in old_files:
    #     if os.path.exists(os.path.join('logs', filename)):
    #         os.remove(os.path.join('logs', filename))


def delete_pipeline(now):
    pass

def push_table():



if __name__ == '__main__':
    gauth = GoogleAuth()
    gauth.LocalWebserverAuth()

    drive = GoogleDrive(gauth)

    folderName = 'hft_logs'
    logs_folder_id = '1OXIgEzY2zJKHqc5EbCqTVtLnJQaM_XmJ'

    while True:
        now = datetime.utcnow() + timedelta(hours=7, minutes=20)
        print(now)
        if now.hour == 0 and now.minute >= 30:
            upload_pipeline(now)
            delete_pipeline(now)
            push_table()
        time.sleep(60)
