from azure.storage.queue import QueueClient
from azure.identity import DefaultAzureCredential, InteractiveBrowserCredential
from typing import Union
import os

class Queue():
    
    def __init__(self, endpoint:str) -> None:
        if os.getenv('MSI_SECRET'):
            key = DefaultAzureCredential()
        else:
            key = InteractiveBrowserCredential()
        queue_client = QueueClient.from_queue_url(queue_url=endpoint, credential=key)
        self.queue_client = queue_client
        self.endpoint = endpoint
        
    def dequeue(self)-> Union[str, None]:
        '''
        dqueue the last message in the queue return None if no message is present.
        '''
        queue_client = self.queue_client
        msg = queue_client.receive_message()
        if msg:
            text = msg.content
            queue_client.delete_message(msg)
            msg = text
        return msg

    def enqueue(self, msg: str)-> None:
        '''
        send a message to the queue.
        '''
        queue_client = self.queue_client
        queue_client.send_message(msg)
        return

    def __len__(self)-> int:
        queue_client = self.queue_client
        properties = queue_client.get_queue_properties()
        count = properties.approximate_message_count
        return count
    
    def __str__(self)-> str:
        return self.endpoint