import os
import yaml
from azure.storage.blob import ContainerClient
from azure.storage.table import TableService
import numpy as np
from dotenv import load_dotenv
import os

class InspectAzureWeedsImageRepo:
    def __init__(self, imageList):
        self.imageList = imageList
        self.imageCites = np.zeros(len(imageList), dtype=int)

    def matchListandURL(self, imageName):
        if (imageName not in self.imageList):
            print(imageName)


def load_config():
    dir_root = os.path.dirname(os.path.abspath(__file__))
    with open(dir_root + "/config.yaml", "r") as yamlfile:
        return yaml.load(yamlfile, Loader=yaml.FullLoader)

def connect_to_azure_container(connection_string, container_name):
    container = ContainerClient.from_connection_string(connection_string, container_name)
    return container

def connect_to_azure_tables(connection_string):
    return TableService(connection_string=connection_string)

load_dotenv()
account_connection_string = os.getenv("STORAGE_CONNECTION_STRING") or ""
if account_connection_string == "":
    print("You need to create a .env file with 'STORAGE_CONNECTION_STRING=' followed by a SAS key")
    exit()

config = load_config()
container = connect_to_azure_container(account_connection_string, config["container"])
imageList = list(container.list_blob_names())
tables = connect_to_azure_tables(config["azure_storage_connection_string"])
statePlants = tables.query_entities('wirmastermeta', filter="UsState eq 'NC'")
inspectorObj = InspectAzureWeedsImageRepo(imageList)


for plant in statePlants:
    MasterRefID = str(plant['RowKey'])
    MasterRefIDimagematchs = list(tables.query_entities('wirimagerefs',
                                                           filter="MasterRefID eq '" + MasterRefID + "'"))
    for imageRef in MasterRefIDimagematchs:
        imageURL = imageRef['ImageURL']
        imageName = imageURL[-12:]
        if imageName[-3:] == 'ARW':
            inspectorObj.matchListandURL(imageName)



