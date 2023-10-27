import paramiko
import io
import datetime
from typing import Dict
from pytz import timezone
import logging

def connect(host: str, username: str, password: str)-> paramiko.SFTPClient:
    transport = paramiko.Transport((host, 22))
    transport.connect(username=username, password=password)
    return paramiko.SFTPClient.from_transport(transport)

def read_file(sftp: paramiko.SFTPClient, path: str)-> bytes:
    with sftp.file(path, 'r') as file:
        b = file.read()
    return b

def check_updates(time: datetime.datetime, sftp: paramiko.SFTPClient, folder_path: str='.')-> bool:
    m_times = [attr.st_mtime for attr in sftp.listdir_attr(folder_path)]
    logging.info(m_times)
    if len(m_times) == 0:
        return False
    else:
        m_time = datetime.datetime.fromtimestamp(max(m_times), tz=timezone('Europe/Rome'))
        logging.info(m_time.time().strftime('%H:%M:%S'))
        return time <= m_time

def collect_file_bytes(sftp: paramiko.SFTPClient, path: str='.')-> Dict[str, bytes]:
    files = sftp.listdir(path=path)
    res = {file_name: read_file(sftp=sftp, path=file_name) for file_name in files}
    return res

def clear(sftp: paramiko.SFTPClient, path: str='.')-> None:
    files = sftp.listdir(path=path)
    for path in files:
        sftp.remove(path)
    return