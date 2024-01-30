import os
from azure.eventgrid import EventGridEvent, EventGridPublisherClient
from azure.identity import InteractiveBrowserCredential, DefaultAzureCredential

def get_topic_client(endpoint: str)-> EventGridPublisherClient:
    if os.getenv('MSI_SECRET'):
        key = DefaultAzureCredential()
    else:
        key = InteractiveBrowserCredential()
    client = EventGridPublisherClient(endpoint=endpoint, credential=key)
    return client

def send(client: EventGridPublisherClient, subject: str, event_type: str, data: str, data_version: str):
    event = EventGridEvent(subject=subject, event_type=event_type, data=data, data_version=data_version)
    client.send(event)