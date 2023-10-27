from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import CreateRunResponse
from azure.identity import DefaultAzureCredential
import os
from typing import Dict

_RG = "DWH"
_factory_name = "DWH-ErselDataFactory"
_client = DataFactoryManagementClient(credential=DefaultAzureCredential(), subscription_id=os.getenv('subscriptionId'))

def run_pipeline(name: str, parameters: Dict[str, str] = None)-> CreateRunResponse:
    _client.pipelines.create_run(resource_group_name=_RG, factory_name=_factory_name, pipeline_name=name, parameters=parameters)