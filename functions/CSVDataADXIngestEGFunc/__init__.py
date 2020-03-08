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


SOURCE_CSV_BLOB_ACCOUNT=''
SOURCE_CSV_FILE_BLOB_KEY=""
SOURCE_CSV_CONTAINER=""
SOURCE_CSV_FILE_TOKEN=""


# ADX CONFIG
APP_AAD_TENANT_ID = ""
APP_CLIENT_ID = ''
APP_CLIENT_SECRETS=''

DATA_INGESTION_URI = "https://[KUSTO Ingestion ].kusto.windows.net:443;Federated Security=True;Application Client Id="+APP_CLIENT_ID+";Application Key="+APP_CLIENT_SECRETS
DATABASE  = ''
DESTINATION_TABLE = ""

APP_INSIGHT_ID=""
APP_INSIGHT_INGEST_EVENT_NAME="EVENTGRID_INGEST_CSV"
APP_INSIGHT_INGEST_RECORDS_COUNT_NAME="INGEST_CSV_SOURCE_FILES_COUNT"

LOG_MESSAGE_HEADER="[Ingest Blob EventGrid]"
MAIN_INGESTION_STATUS_COSMOS_DOC_TYPE="INGESTION_EVENTGRID"
PROCESS_PROGRAM_NAME="INGESTION_EVENTGRID_V01A"

EVENT_SUBJECT_FILTER_REGEX="^((?!data_dsa_telemetry).)*$"

DESTINATION_TABLE_COLUMN_MAPPING = "CSV_Mapping01A"
IS_FLUSH_IMMEDIATELY=True

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

        telemetry_block_blob_service = BlockBlobService(account_name=SOURCE_OSMETRICS_BLOB_ACCOUNT,   account_key=SOURCE_OSMETRICS_FILE_BLOB_KEY)
        filepath=get_file_path(event.subject)
        container_name=get_container_name(event.subject)

        logging.info('{} filepath: {}'.format(LOG_MESSAGE_HEADER,filepath))
        logging.info('{} container_name: {}'.format(LOG_MESSAGE_HEADER, container_name))   
        ingest_to_ADX(filepath,telemetry_block_blob_service,container_name,SOURCE_OSMETRICS_BLOB_ACCOUNT, tc)
    else:
        logging.info("{} Subject : {} does not match regular express {}. Skip process. ".format(LOG_MESSAGE_HEADER,event.subject,EVENT_SUBJECT_FILTER_REGEX))



def get_config_values():
    global COSMOS_URL,COSMOS_KEY,COSMOS_DATABASE,COSMOS_CONTAINER
    global SOURCE_OSMETRICS_BLOB_ACCOUNT,SOURCE_OSMETRICS_FILE_BLOB_KEY,SOURCE_OSMETRICS_CONTAINER,SOURCE_OSMETRICS_FILE_TOKEN
    global APP_AAD_TENANT_ID,APP_CLIENT_ID,APP_CLIENT_SECRETS,DATA_INGESTION_URI,DATABASE,DESTINATION_TABLE
    global APP_INSIGHT_ID,APP_INSIGHT_INGEST_EVENT_NAME,APP_INSIGHT_INGEST_RECORDS_COUNT_NAME
    global LOG_MESSAGE_HEADER,MAIN_INGESTION_STATUS_COSMOS_DOC_TYPE,PROCESS_PROGRAM_NAME,EVENT_SUBJECT_FILTER_REGEX
    global DESTINATION_TABLE_COLUMN_MAPPING,IS_FLUSH_IMMEDIATELY

    # COSMOS CONFIG
    COSMOS_URL= os.getenv("COSMOS_URL",COSMOS_URL)
    COSMOS_KEY= os.getenv("COSMOS_KEY",COSMOS_KEY)
    COSMOS_DATABASE=os.getenv("COSMOS_DATABASE",COSMOS_DATABASE)
    COSMOS_CONTAINER=os.getenv("COSMOS_CONTAINER",COSMOS_CONTAINER)

  
    SOURCE_OSMETRICS_BLOB_ACCOUNT=os.getenv("SOURCE_OSMETRICS_BLOB_ACCOUNT",SOURCE_OSMETRICS_BLOB_ACCOUNT)
    SOURCE_OSMETRICS_FILE_BLOB_KEY=os.getenv("SOURCE_OSMETRICS_FILE_BLOB_KEY",SOURCE_OSMETRICS_FILE_BLOB_KEY)
    SOURCE_OSMETRICS_CONTAINER=os.getenv("SOURCE_OSMETRICS_CONTAINER",SOURCE_OSMETRICS_CONTAINER)
    SOURCE_OSMETRICS_FILE_TOKEN=os.getenv("SOURCE_OSMETRICS_FILE_TOKEN",SOURCE_OSMETRICS_FILE_TOKEN)

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
    DESTINATION_TABLE_COLUMN_MAPPING=os.getenv("DESTINATION_TABLE_COLUMN_MAPPING",DESTINATION_TABLE_COLUMN_MAPPING)
    IS_FLUSH_IMMEDIATELY=bool(os.getenv("IS_FLUSH_IMMEDIATELY",IS_FLUSH_IMMEDIATELY))
    #logging.info(f'My app setting value:{my_app_setting_value}')


def get_container_name(fullfilepath):
    frefixword='/containers/'
    suffixword='/blobs/'
    return fullfilepath[fullfilepath.find(frefixword)+len(frefixword):fullfilepath.find(suffixword)]

def get_file_path(fullfilepath):
    frefixword='/blobs/'
    return fullfilepath[fullfilepath.find(frefixword)+len(frefixword):]

def get_uuids_from_csv(telemetry_block_blob_service,container_name,filepath):
    with io.BytesIO() as output_blob:
        telemetry_block_blob_service.get_blob_to_stream(container_name, filepath, output_blob)
        z = zipfile.ZipFile(output_blob)
        foo2 = z.open(z.infolist()[0])
        lines= foo2.readlines()
        content="\n".join(line.strip().decode("utf-8") for line in lines) 
        osmetric_df = pd.read_csv(io.StringIO(content))
        osmetric_df.sort_values('Universal_datetime')
        osmetric_df['dataTimestamp']=pd.to_datetime(osmetric_df['Universal_datetime']).values.astype(int) / 10**6
        record_num=len( osmetric_df.index)
        print(LOG_MESSAGE_HEADER+ " : Total Doc # : "+str(len( osmetric_df.index))+osmetric_df['Universal_datetime'][0]+osmetric_df['Universal_datetime'][record_num-1])
        print(LOG_MESSAGE_HEADER+ " : Total Doc # : "+str(len( osmetric_df.index))+'  '+str(osmetric_df['dataTimestamp'][0])+'  '+str(osmetric_df['dataTimestamp'][record_num-1]))
      
        return osmetric_df['vm_uuid'][0],osmetric_df['config_uuid'][0],osmetric_df['deploy_uuid'][0],len(content),osmetric_df['dataTimestamp'][0],osmetric_df['dataTimestamp'][record_num-1],record_num

#def save_COSMOS_log(vm_uuid,deploy_uuid,config_uuid,file_path,min_datatime,max_datatime, total_records,blob_container_id ,tc):
def save_COSMOS_log(vm_uuid,deploy_uuid,config_uuid,file_path,min_datatime,max_datatime, total_records,ingest_source_id,blob_account,blob_container_name, tc):

    url=COSMOS_URL
    key=COSMOS_KEY
    client = cosmos_client.CosmosClient(url, {'masterKey': key})
    database_id=COSMOS_DATABASE
    container_id=COSMOS_CONTAINER

    database_link = 'dbs/' + database_id
    collection_link = database_link + '/colls/' + container_id

    filename=get_filename(file_path)
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
        win_telemetry_info["ingestions"]=[]
        win_telemetry_info["blob_account"]=blob_account
        win_telemetry_info["blob_container_name"]=blob_container_name   

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

def ingest_to_ADX(filepath, telemetry_block_blob_service, container_name,blob_account, tc):
    ingest_source_id=str(uuid.uuid4())
    #file_size=BlockBlobService.get_blob_properties(telemetry_block_blob_service,container_name,filepath).properties.content_length
    #print (filepath+" File Size "+str(file_size))
    
    KCSB_INGEST = KustoConnectionStringBuilder.with_aad_device_authentication(DATA_INGESTION_URI)
    KCSB_INGEST.authority_id = APP_AAD_TENANT_ID
   
    vm_uuid,config_uuid,deploy_uuid,file_size, min_datatime, max_datatime, total_records=get_uuids_from_csv(telemetry_block_blob_service,container_name,filepath)
    dropByTag= vm_uuid+'_'+config_uuid+'_'+deploy_uuid
    
    INGESTION_CLIENT = KustoIngestClient(KCSB_INGEST)
    INGESTION_PROPERTIES  = IngestionProperties(database=DATABASE, table=DESTINATION_TABLE, dataFormat=DataFormat.CSV, mappingReference=DESTINATION_TABLE_COLUMN_MAPPING, additionalProperties={'ignoreFirstRecord': 'true','reportMethod':'QueueAndTable'}, reportLevel=ReportLevel.FailuresAndSuccesses, dropByTags=[dropByTag],flushImmediately=IS_FLUSH_IMMEDIATELY)
    
    BLOB_PATH = "https://" + SOURCE_OSMETRICS_BLOB_ACCOUNT + ".blob.core.windows.net/" + SOURCE_OSMETRICS_CONTAINER + "/" + filepath + SOURCE_OSMETRICS_FILE_TOKEN
    #print (BLOB_PATH,' ',str(file_size))    
    BLOB_DESCRIPTOR = BlobDescriptor(BLOB_PATH, file_size, ingest_source_id)  # 10 is the raw size of the data in bytes
   
    INGESTION_CLIENT.ingest_from_blob(BLOB_DESCRIPTOR,ingestion_properties=INGESTION_PROPERTIES)
    


    tc.context.properties["ingest_source_id"]=str(ingest_source_id)

    doc_id=save_COSMOS_log(vm_uuid,deploy_uuid,config_uuid,filepath,min_datatime,max_datatime, total_records,ingest_source_id,blob_account,container_name, tc)

    tc.track_event(APP_INSIGHT_INGEST_EVENT_NAME, { 'FILE_PATH': filepath,'DOC_ID':doc_id,"SOURCE_ID":ingest_source_id }, { 'TOTOAL_RECORDS': total_records, 'FILE_SIZE':file_size,'MIN_DATETIME':min_datatime,'MAX_DATETIME': max_datatime })
    log_msg="{} Done queuing up ingestion with Azure Data Explorer {}, Ingest SourceID {}".format(LOG_MESSAGE_HEADER,filepath,ingest_source_id)
    print(log_msg)
    tc.track_trace(log_msg)
    tc.flush()
 
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
