import logging

import pandas as pd
import time,datetime
from io import StringIO
from azure.storage.blob import BlockBlobService
from pandas.core.indexes.api import _union_indexes
import json
import codecs
import re
import os,sys
import copy
import io
import zipfile
import re 
import uuid

import azure.functions as func

from azure.kusto.data.request import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.kusto.data.helpers import dataframe_from_result_table
from azure.kusto.ingest import KustoIngestClient, IngestionProperties, FileDescriptor, BlobDescriptor, DataFormat, ReportLevel, ReportMethod
from azure.kusto.ingest import (
    BlobDescriptor,
    IngestionProperties,
    DataFormat,
    CsvColumnMapping,
    JsonColumnMapping,
    ReportLevel,
    ReportMethod,
    ValidationPolicy,
    ValidationOptions,
    ValidationImplications,
)

import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.http_constants as http_constant

from applicationinsights import TelemetryClient


# COSMOS CONFIG
COSMOS_URL="https://[COSMOS Server].documents.azure.com:443/"
COSMOS_KEY=""
COSMOS_DATABASE='[COSMOS DATABASE]'
COSMOS_CONTAINER='[]'


CONFIG_FILE_BLOB_ACCOUNT=""
CONFIG_FILE_CONTAINER = ""
CONFIG_FILE_TOKEN = ""
CONFIG_FILE_BLOB_KEY=""

CLEAN_FILE_BLOB_ACCOUNT=""
CLEAN_FILE_CONTAINER = ""
CLEAN_FILE_TOKEN = ""
CLEAN_FILE_BLOB_KEY=""


# ADX CONFIG
APP_AAD_TENANT_ID = "[ADX Service Priciple TENANT ID]"
APP_CLIENT_ID = '[ADX Service Priciple Client ID]'
APP_CLIENT_SECRETS='[ADX Service Priciple Secrets]'

DATA_INGESTION_URI = "https://[KUSTO Ingestion ].kusto.windows.net:443;Federated Security=True;Application Client Id="+APP_CLIENT_ID+";Application Key="+APP_CLIENT_SECRETS

DATABASE  = ''
DESTINATION_TABLE = ""

APP_INSIGHT_ID=""
APP_INSIGHT_INGEST_EVENT_NAME="EVENTGRID_INGEST_CONFIG_JSON"
APP_INSIGHT_INGEST_RECORDS_COUNT_NAME="INGEST_CONFIG_JSON_SOURCE_FILES_COUNT"

LOG_MESSAGE_HEADER="[Config-Ingest Blob EventGrid]"
MAIN_INGESTION_STATUS_COSMOS_DOC_TYPE="CONFIG_INGESTION_EVENTGRID"
PROCESS_PROGRAM_NAME="CONFIG_INGESTION_EVENTGRID_V01A"

EVENT_SUBJECT_FILTER_REGEX="^((?!_telemetry).)*$"
IS_FLUSH_IMMEDIATELY=True #True or False

vm_uuid=""
deploy_uuid=""
config_uuid=""


def main(event: func.EventGridEvent):
    result = json.dumps({
        'id': event.id,
        'data': event.get_json(),
        'topic': event.topic,
        'subject': event.subject,
        'event_type': event.event_type,
    })

    logging.info('{} Python EventGrid trigger processed an event:{}'.format(LOG_MESSAGE_HEADER ,result))

    get_config_values()
    regexp = re.compile(EVENT_SUBJECT_FILTER_REGEX)

    if regexp.search(event.subject): # Check if file path match criteria
        tc = TelemetryClient(APP_INSIGHT_ID)
        tc.context.application.ver = '1.0'
        tc.context.properties["PROCESS_PROGRAM"]=PROCESS_PROGRAM_NAME
        tc.context.properties["PROCESS_START"]=time.time()                                        
        tc.track_trace('{} STRAT RUN EVENTGRID INGEST TELEMETRY JSON DATA from folder {} '.format(LOG_MESSAGE_HEADER,result))
        tc.flush()

        configfile_block_blob_service = BlockBlobService(account_name=CONFIG_FILE_BLOB_ACCOUNT, account_key=CONFIG_FILE_BLOB_KEY)
        cleanfile_block_blob_service = BlockBlobService(account_name=CLEAN_FILE_BLOB_ACCOUNT, account_key=CLEAN_FILE_BLOB_KEY)
        filepath=get_file_path(event.subject)
        container_name=get_container_name(event.subject)
        config_file_name=get_filename(filepath)
        logging.info('{} filepath: {}'.format(LOG_MESSAGE_HEADER,filepath))
        logging.info('{} container_name: {}'.format(LOG_MESSAGE_HEADER, container_name))   

        config_file_name_utf8, filesize,vm_uuid,deploy_uuid,config_uuid=generate_UTF8_config_json(configfile_block_blob_service,CONFIG_FILE_CONTAINER,filepath,cleanfile_block_blob_service,CLEAN_FILE_CONTAINER )  
        ingest_to_ADX(config_file_name_utf8, cleanfile_block_blob_service, CLEAN_FILE_CONTAINER, CLEAN_FILE_BLOB_ACCOUNT,filesize,tc,vm_uuid,deploy_uuid,config_uuid)

    else:
        logging.info("{} Subject : {} does not match regular express {}. Skip process. ".format(LOG_MESSAGE_HEADER,event.subject,EVENT_SUBJECT_FILTER_REGEX))



def get_config_values():
    global COSMOS_URL,COSMOS_KEY,COSMOS_DATABASE,COSMOS_CONTAINER
    global CONFIG_FILE_BLOB_ACCOUNT,CONFIG_FILE_BLOB_KEY,CONFIG_FILE_CONTAINER,CONFIG_FILE_TOKEN,CLEAN_FILE_BLOB_ACCOUNT,CLEAN_FILE_CONTAINER,CLEAN_FILE_TOKEN,CLEAN_FILE_BLOB_KEY
    global APP_AAD_TENANT_ID,APP_CLIENT_ID,APP_CLIENT_SECRETS,DATA_INGESTION_URI,DATABASE,DESTINATION_TABLE
    global APP_INSIGHT_ID,APP_INSIGHT_INGEST_EVENT_NAME,APP_INSIGHT_INGEST_RECORDS_COUNT_NAME
    global LOG_MESSAGE_HEADER,MAIN_INGESTION_STATUS_COSMOS_DOC_TYPE,PROCESS_PROGRAM_NAME,EVENT_SUBJECT_FILTER_REGEX
    global IS_FLUSH_IMMEDIATELY

    # COSMOS CONFIG
    COSMOS_URL= os.getenv("COSMOS_URL",COSMOS_URL)
    COSMOS_KEY= os.getenv("COSMOS_KEY",COSMOS_KEY)
    COSMOS_DATABASE=os.getenv("COSMOS_DATABASE",COSMOS_DATABASE)
    COSMOS_CONTAINER=os.getenv("COSMOS_CONTAINER",COSMOS_CONTAINER)

    CONFIG_FILE_BLOB_ACCOUNT=os.getenv("CONFIG_FILE_BLOB_ACCOUNT",CONFIG_FILE_BLOB_ACCOUNT)
    CONFIG_FILE_BLOB_KEY=os.getenv("CONFIG_FILE_BLOB_KEY",CONFIG_FILE_BLOB_KEY)
    CONFIG_FILE_CONTAINER=os.getenv("CONFIG_FILE_CONTAINER",CONFIG_FILE_CONTAINER)
    CONFIG_FILE_TOKEN=os.getenv("CONFIG_FILE_TOKEN",CONFIG_FILE_TOKEN)

    CLEAN_FILE_BLOB_ACCOUNT=os.getenv("CLEAN_FILE_BLOB_ACCOUNT",CLEAN_FILE_BLOB_ACCOUNT)
    CLEAN_FILE_CONTAINER=os.getenv("CLEAN_FILE_CONTAINER",CLEAN_FILE_CONTAINER)
    CLEAN_FILE_TOKEN=os.getenv("CLEAN_FILE_TOKEN",CLEAN_FILE_TOKEN)
    CLEAN_FILE_BLOB_KEY=os.getenv("CLEAN_FILE_BLOB_KEY",CLEAN_FILE_BLOB_KEY)


    # ADX CONFIG
    APP_AAD_TENANT_ID = os.getenv("APP_AAD_TENANT_ID",APP_AAD_TENANT_ID)
    APP_CLIENT_ID = os.getenv("APP_CLIENT_ID",APP_CLIENT_ID)
    APP_CLIENT_SECRETS=os.getenv("APP_CLIENT_SECRETS",APP_CLIENT_SECRETS)
    DATA_INGESTION_URI =os.getenv("DATA_INGESTION_URI",DATA_INGESTION_URI)
    DATABASE  = os.getenv("DATABASE",DATABASE)
    DESTINATION_TABLE = os.getenv("DESTINATION_TABLE",DESTINATION_TABLE)

    APP_INSIGHT_ID=os.getenv("APP_INSIGHT_ID",APP_INSIGHT_ID)
    APP_INSIGHT_INGEST_EVENT_NAME=os.getenv("APP_INSIGHT_INGEST_EVENT_NAME",APP_INSIGHT_INGEST_EVENT_NAME)
    APP_INSIGHT_INGEST_RECORDS_COUNT_NAME=os.getenv("APP_INSIGHT_INGEST_RECORDS_COUNT_NAME",APP_INSIGHT_INGEST_RECORDS_COUNT_NAME)

    LOG_MESSAGE_HEADER=os.getenv("LOG_MESSAGE_HEADER",LOG_MESSAGE_HEADER)

    MAIN_INGESTION_STATUS_COSMOS_DOC_TYPE=os.getenv("MAIN_INGESTION_STATUS_COSMOS_DOC_TYPE",MAIN_INGESTION_STATUS_COSMOS_DOC_TYPE)
    PROCESS_PROGRAM_NAME=os.getenv("PROCESS_PROGRAM_NAME",PROCESS_PROGRAM_NAME)

    EVENT_SUBJECT_FILTER_REGEX=os.getenv("EVENT_SUBJECT_FILTER_REGEX",EVENT_SUBJECT_FILTER_REGEX)
    IS_FLUSH_IMMEDIATELY=bool(os.getenv("IS_FLUSH_IMMEDIATELY",IS_FLUSH_IMMEDIATELY))
 
    #logging.info(f'My app setting value:{my_app_setting_value}')



def ingest_to_ADX(filepath, telemetry_block_blob_service, container_name, blob_account, file_size, tc,vm_uuid,deploy_uuid,config_uuid):
    ingest_source_id=str(uuid.uuid4())
    KCSB_INGEST = KustoConnectionStringBuilder.with_aad_device_authentication(DATA_INGESTION_URI)
    KCSB_INGEST.authority_id = APP_AAD_TENANT_ID
    INGESTION_CLIENT = KustoIngestClient(KCSB_INGEST)
    ing_map=[JsonColumnMapping("vm_uuid", "$.vm_uuid", "string"),
             JsonColumnMapping("deploy_uuid", "$.deployment_description[0].deploy_uuid", "string"),
             JsonColumnMapping("config_uuid", "$.vm_configuration[0].config_uuid", "string"),
             JsonColumnMapping("rawdata", "$", "dynamic")]
        
    INGESTION_PROPERTIES  = IngestionProperties(database=DATABASE, table=DESTINATION_TABLE, dataFormat=DataFormat.JSON, ingestionMapping=ing_map, reportLevel=ReportLevel.FailuresAndSuccesses,flushImmediately=IS_FLUSH_IMMEDIATELY)                                                                                                                                                          

    print("Database {} Tabele {}".format(DATABASE,DESTINATION_TABLE))
    
    BLOB_PATH = "https://" + blob_account + ".blob.core.windows.net/" + container_name + "/" + filepath + CLEAN_FILE_TOKEN

    print (BLOB_PATH,' ',str(file_size), ingest_source_id)
    BLOB_DESCRIPTOR = BlobDescriptor(BLOB_PATH, file_size, ingest_source_id) # 10 is the raw size of the data in bytes
    INGESTION_CLIENT.ingest_from_blob(BLOB_DESCRIPTOR,ingestion_properties=INGESTION_PROPERTIES)
    tc.context.properties["ingest_source_id"]=ingest_source_id

    min_datatime=0
    max_datatime=0
    total_records=1

    doc_id=save_COSMOS_log(vm_uuid,deploy_uuid,config_uuid,filepath,min_datatime,max_datatime, total_records,ingest_source_id,blob_account,container_name, tc)

    tc.track_event(APP_INSIGHT_INGEST_EVENT_NAME, { 'FILE_PATH': filepath,'DOC_ID':doc_id,"SOURCE_ID":ingest_source_id }, { 'TOTOAL_RECORDS': total_records, 'FILE_SIZE':file_size,'MIN_DATETIME':min_datatime,'MAX_DATETIME': max_datatime })
    log_msg="{} Done queuing up ingestion with Azure Data Explorer {}, Ingest SourceID {}".format(LOG_MESSAGE_HEADER,filepath,ingest_source_id)
    print(log_msg)
    tc.track_trace(log_msg)
    tc.flush()


 
#def save_COSMOS_log(vm_uuid,deploy_uuid,config_uuid,file_path,min_datatime,max_datatime, total_records,blob_conatiner_name ,tc):
def save_COSMOS_log(vm_uuid,deploy_uuid,config_uuid,file_path,min_datatime,max_datatime, total_records,ingest_source_id,blob_account,blob_container_name, tc):

    url=COSMOS_URL
    key=COSMOS_KEY
    client = cosmos_client.CosmosClient(url, {'masterKey': key})
    database_id=COSMOS_DATABASE
    container_id=COSMOS_CONTAINER

    database_link = 'dbs/' + database_id
    collection_link = database_link + '/colls/' + container_id

    #doc_id=vm_uuid+'_'+deploy_uuid+'_'+config_uuid+'_'+filename
    doc_id=get_doc_id(blob_container_name,file_path)
    print (doc_id)
    
    doc_link = collection_link + '/docs/' + doc_id
    options = {}
    options['enableCrossPartitionQuery'] = True
    options['maxItemCount'] = 5
    options['partitionKey'] = vm_uuid
    
    win_telemetry_info=None
    try:
        win_telemetry_info = client.ReadItem(doc_link,options)
    except:
        print("New Process Log Doc")

    if(win_telemetry_info is not None):
        print ("Find Existing Process Log Doc ")
    else: # New process log
        win_telemetry_info={}
        win_telemetry_info["id"]=doc_id
        win_telemetry_info["process_type"]=PROCESS_PROGRAM_NAME
        win_telemetry_info["DOC_TYPE"]=MAIN_INGESTION_STATUS_COSMOS_DOC_TYPE  #DOC_TYPE
        win_telemetry_info["file_path"]=file_path
        win_telemetry_info["blob_account"]=blob_account
        win_telemetry_info["blob_container_name"]=blob_container_name  
        win_telemetry_info["ingestions"]=[]
        
    ingestion_info={}
    ingestion_info["source_id"]=ingest_source_id     
    ingestion_info["ingest_trigger_time"]=time.time()
    ingestion_info["min_datatime"]=min_datatime
    ingestion_info["max_datatime"]=max_datatime
    ingestion_info["total_records"]=total_records
    ingestion_info["status"]='PENDING'
    
    
    win_telemetry_info["ingestions"].append(ingestion_info)
    ingestion_info['LATEST_UPDATE_TIMESTAMP']=time.time()

    tc.track_metric(APP_INSIGHT_INGEST_RECORDS_COUNT_NAME, total_records)
    tc.flush()
    
    client.UpsertItem(collection_link,win_telemetry_info,options)
    return doc_id


def generate_UTF8_config_json(configfile_block_blob_service,config_file_container,filepath,cleanfile_block_blob_service,clean_file_container ):

        print ("{}  Read Config file container {} filepath {}".format(LOG_MESSAGE_HEADER,config_file_container,filepath))
        configstring = configfile_block_blob_service.get_blob_to_text(config_file_container, filepath, encoding='utf-8').content
        #print (configstring)
        #decoded_data=codecs.decode(configstring.encode(), 'utf-8-sig')
        config_obj=json.loads(configstring)


        print("{}  Read Config file size {} ".format(LOG_MESSAGE_HEADER,str(len(config_obj))))
        filesize=len( json.dumps(config_obj))
        clean_file_path=filepath.replace('.json','_ingest.json')
        cleanfile_block_blob_service.create_blob_from_text(clean_file_container,clean_file_path, json.dumps(config_obj))
        print ("{}  Write config to container {}, file path {}".format(LOG_MESSAGE_HEADER,clean_file_path,clean_file_path))
        return clean_file_path, filesize, vm_uuid,deploy_uuid,config_uuid

def get_container_name(fullfilepath):
    frefixword='/containers/'
    suffixword='/blobs/'
    return fullfilepath[fullfilepath.find(frefixword)+len(frefixword):fullfilepath.find(suffixword)]

def get_file_path(fullfilepath):
    frefixword='/blobs/'
    return fullfilepath[fullfilepath.find(frefixword)+len(frefixword):]


def get_filename(filepath):
    path_secs=filepath.split('/')
    filename=path_secs[len(path_secs)-1]
    return filename


def get_doc_id(container_id,file_path):
    file_path=file_path[len(file_path)-min(len(file_path),200):]  # COSMOSDB Can only acceppet doc id length less than 255 characters
    return (container_id+'_'+file_path).replace('/','_').replace('\\','_').replace('?','_').replace('#','_')
