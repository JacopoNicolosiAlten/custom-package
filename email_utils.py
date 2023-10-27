import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from typing import Dict
import os
import requests
import json
from azure.identity import DefaultAzureCredential

_email_server_name = "smtp.gmail.com" # name of the server that hosts the email system of the sender
_email_server_port = 465 # port number for the connection to the server above
_email_password = os.getenv("emailPassword") # name of the environment variable in which the email password is stored
_sender_email_address = "pipelineagresso.notifier@gmail.com" # email address of the sender
_recipient_email_address = "jacopo.nicolosi@alten.it"

def send_plain_text(subject: str, text: str, server: str=_email_server_name, port: int=_email_server_port, email_from: str=_sender_email_address, password: str=_email_password, email_to: list[str]=[_recipient_email_address], attachments: Dict[str, bytes] = dict()) -> None:
    '''
    Send a plain text as an email from the specified address to the given list of addresses.
    It requires info about the server name, the port and the password of the sender account. 
    '''
    message = MIMEMultipart("alternative")
    message["Subject"] = subject
    message["From"] = email_from
    message["To"] = ', '.join(email_to)
    body = MIMEText(text, 'plain')
    message.attach(body)
    for file_name, bytes in attachments.items():
        part = MIMEApplication(bytes, Name=file_name)
        part['Content-Disposition'] = f'attachment; filename="{file_name}"'
        message.attach(part)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(host=server, port=port, context=context) as server:
        server.login(user=email_from, password=password)
        server.sendmail(msg=message.as_string(), from_addr=email_from, to_addrs=email_to)
    return

def send_email_lapp(subject: str, text:str, email_to: str=_recipient_email_address)-> requests.Response:
    body = {"subject": subject, "text": text, "email_to": email_to}
    token = DefaultAzureCredential().get_token("https://management.azure.com/.default").token
    headers = {"Authorization": f"Bearer {token}",
        "Content-Type": "application/json"}
    response = requests.post(url="https://prod-208.westeurope.logic.azure.com:443/workflows/c8c0fa9ea19d49d3862430de1fdf1108/triggers/manual/paths/invoke?api-version=2016-10-01",
        json=body, headers=headers)
    if not response.ok:
        raise Exception(f'The request to the logic app failed with code {response.status_code}.')
    return response