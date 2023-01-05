from tablestorageaccount import TableStorageAccount
import numpy as np
from dotenv import load_dotenv
import os

load_dotenv()
account_connection_string = os.getenv("STORAGE_CONNECTION_STRING") or ""
if account_connection_string == "":
    print("You need to create a .env file with 'STORAGE_CONNECTION_STRING=' followed by a SAS key")
    exit()

# Split into key=value pairs removing empties, then split the pairs into a dict
config = dict(s.split('=', 1) for s in account_connection_string.split(';') if s)
# Authentication
account_name = config.get('AccountName')
account_key = config.get('AccountKey')
# Basic URL Configuration
table_endpoint_suffix = config.get('EndpointSuffix')
if table_endpoint_suffix == None:
    print("In here")
    table_endpoint = config.get('TableEndpoint')
    table_prefix = '.table.'
    table_start_index = table_endpoint.find(table_prefix)
    table_end_index = table_endpoint.endswith(':') and len(table_endpoint) or table_endpoint.rfind(':')
    table_endpoint_suffix = table_endpoint[table_start_index + len(table_prefix):table_end_index]

account = TableStorageAccount(account_name=account_name, connection_string=account_connection_string,
                              endpoint_suffix=table_endpoint_suffix)
table_service = account.create_table_service()
print('List tables')
tables = table_service.list_tables()
for table in tables:
    print('\Table Name: ' + table.name)

TXuploads = table_service.query_entities('wirmastermeta', filter="UsState eq 'TX'")
no_match = []
species_count = {"Common sunflower": np.array([[0,0,0],[0,0,0],[0,0,0]])}

for TXupload in TXuploads:
    MasterRefID = str(TXupload['RowKey'] + "'")
    MasterRefIDweedmatchs = table_service.query_entities('wirweedsmeta',
                                                     filter="MasterRefID eq '" + MasterRefID)
    MasterRefIDimagesmatchs = table_service.query_entities('wirimagerefs',
                                                     filter="MasterRefID eq '" + MasterRefID)
    MasterRefIDcropmatchs = table_service.query_entities('wircropsmeta',
                                                     filter="MasterRefID eq '" + MasterRefID)

    #print(MasterRefIDweedmatch['WeedType'])
    matchFound = False
    for MasterRefIDweedmatch in MasterRefIDweedmatchs:
        for species in species_count.keys():
            if species == str(MasterRefIDweedmatch['WeedType']):
                if str(MasterRefIDweedmatch['CropOrFallow']) == "Fallow":
                    col = 0
                elif str(MasterRefIDweedmatch['CropType']) == "Soybean":
                    col = 1
                else:
                    col = 2
                if str(MasterRefIDweedmatch['SizeClass']) == "SMALL" or str(MasterRefIDweedmatch['SizeClass']) == "1" or str(MasterRefIDweedmatch['SizeClass']) == "Small":
                    row = 0
                elif str(MasterRefIDweedmatch['SizeClass']) == "MEDIUM" or str(MasterRefIDweedmatch['SizeClass']) == "2" or str(MasterRefIDweedmatch['SizeClass']) == "Medium":
                    row = 1
                else:
                    row = 2
                species_count[species][row][col] += 1
                matchFound = True

            #elif species == str(MasterRefIDweedmatch['CropType']):
            #    if str(MasterRefIDweedmatch['CropOrFallow']) == "Fallow":
            #        col = 0
            #    elif str(MasterRefIDweedmatch['CropType']) == "Soybean":
            #        col = 1
            #    else:
            #        col = 2
            #    if str(MasterRefIDweedmatch['SizeClass']) == "SMALL" or str(MasterRefIDweedmatch['SizeClass']) == "1":
            #        row = 0
            #    elif str(MasterRefIDweedmatch['SizeClass']) == "MEDIUM" or str(MasterRefIDweedmatch['SizeClass']) == "2":
            #        row = 1
            #    else:
            #        row = 2
            #    species_count[species][row][col] += 1
            #    matchFound = True

        if not matchFound:
            if str(MasterRefIDweedmatch['CropOrFallow']) == "Fallow":
                col = 0
            elif str(MasterRefIDweedmatch['CropType']) == "Soybean":
                col = 1
            else:
                col = 2
            if str(MasterRefIDweedmatch['SizeClass']) == "SMALL" or str(MasterRefIDweedmatch['SizeClass']) == "1" or str(MasterRefIDweedmatch['SizeClass']) == "Small":
                row = 0
            elif str(MasterRefIDweedmatch['SizeClass']) == "MEDIUM" or str(MasterRefIDweedmatch['SizeClass']) == "2" or str(MasterRefIDweedmatch['SizeClass']) == "Medium":
                row = 1
            else:
                row = 2
            species_count[str(MasterRefIDweedmatch['WeedType'])] = np.array([[0,0,0],[0,0,0],[0,0,0]])
            species_count[str(MasterRefIDweedmatch['WeedType'])][row][col] += 1

    matchFound = False
    for MasterRefIDcropmatch in MasterRefIDcropmatchs:
        try:
            if str(MasterRefIDcropmatch['Height']) == '0.3 – 0.6m':
                row = 0
            elif str(MasterRefIDcropmatch['Height']) == '0.61 – 0.9m':
                row = 1
            else:
                row = 2
        except:
            if str(MasterRefIDcropmatch['SizeClass']) == 'Small':
                row = 0
            elif str(MasterRefIDcropmatch['SizeClass']) == 'Medium':
                row = 1
            else:
                row = 2

        for species in species_count.keys():
            if species == str(MasterRefIDcropmatch['CropName']):
                species_count[species][row] += 1
                matchFound = True
        if not matchFound:
            species_count[str(MasterRefIDcropmatch['CropName'])] = np.array([0, 0, 0])
            species_count[str(MasterRefIDcropmatch['CropName'])][row] += 1

    if len(list(MasterRefIDimagesmatchs)) != 20:
        print(TXupload['RowKey'])

#print(species_count)
for species in species_count.keys():
    print(str(species)+":")
    print(species_count[species])

#print("No match")
#print(no_match)