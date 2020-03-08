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
import uuid 
import traceback
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

from azure.kusto.ingest._resource_manager import _ResourceUri
from azure.kusto.ingest.status import KustoIngestStatusQueues, SuccessMessage, FailureMessage

import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.http_constants as http_constant

from applicationinsights import TelemetryClient

from msrest.authentication import TopicCredentials
from azure.eventgrid import EventGridClient


# COSMOS CONFIG
COSMOS_URL="https://[COSMOS Server].documents.azure.com:443/"
COSMOS_KEY=""
COSMOS_DATABASE='[COSMOS DATABASE]'
COSMOS_CONTAINER='[]'


PROCESSED_TELEMETRY_FOLDER='telemetry_processed'

# ADX CONFIG
APP_AAD_TENANT_ID = "[ADX Service Priciple TENANT ID]"
APP_CLIENT_ID = '[ADX Service Priciple Client ID]'
APP_CLIENT_SECRETS='[ADX Service Priciple Secrets]'
DATA_INGESTION_URI = "https://[KUSTO Ingestion ].kusto.windows.net:443;Federated Security=True;Application Client Id="+APP_CLIENT_ID+";Application Key="+APP_CLIENT_SECRETS

SUCCESS_STATUS="SUCCESS"
FAILURE_STATUS="FAILURE"

APP_INSIGHT_ID="" 
APP_INSIGHT_MAIN_ERROR_EVENT_NAME="MONITOR_ADX_ERROR"
APP_INSIGHT_INGEST_SUCCESS_COUNT_NAME="INGEST_SUCCESS_COUNT"
APP_INSIGHT_INGEST_FAILURE_COUNT_NAME="INGEST_FAILURE_COUNT"
APP_INSIGHT_INGEST_SUCCESS_EVENT_NAME="INGEST_SUCCESS"
APP_INSIGHT_INGEST_FAILURE_EVENT_NAME="INGEST_FAILURE"

LOG_MESSAGE_HEADER="[ADX-MONITOR]"
PROCESS_PROGRAM_NAME="MONITOR_ADX_V001a"

EVENT_GRID_ENDPOINT="[event grid endpoint].eventgrid.azure.net"
EVENT_GRID_KEY=""

COSMOS_CLIENT = cosmos_client.CosmosClient(COSMOS_URL, {'masterKey': COSMOS_KEY})
vm_uuid=""
deploy_uuid=""
config_uuid=""

def get_file_path(IngestionSourcePath):
    blob_basic_urn=".blob.core.windows.net/"
    temp_path=IngestionSourcePath[IngestionSourcePath.find(blob_basic_urn)+len(blob_basic_urn):]
    #print (temp_path)
    file_path=temp_path[temp_path.find('/')+1:]
    #print (file_path)
    return file_path


def get_container_name(IngestionSourcePath):
    blob_basic_urn=".blob.core.windows.net/"
    temp_path=IngestionSourcePath[IngestionSourcePath.find(blob_basic_urn)+len(blob_basic_urn):]
    sec_path=temp_path.split('/')
    return sec_path[0]

def update_COSMOS_status(client,file_path,datetime, status, message,vm_uuid,source_id ,blob_conatiner_name,tc , doc_order,run_id):

    database_id=COSMOS_DATABASE
    container_id=COSMOS_CONTAINER
    
    doc_id=get_doc_id(blob_conatiner_name,file_path)
    database_link = 'dbs/' + database_id
    collection_link = database_link + '/colls/' + container_id
   
    doc_link = collection_link + '/docs/' + doc_id
    
    print('{} doc_link : {}'.format(LOG_MESSAGE_HEADER, doc_link))
    
    options = {}
    options['enableCrossPartitionQuery'] = True
    options['maxItemCount'] = 5
    options['partitionKey'] = vm_uuid

    win_telemetry_info=None
    try:
        win_telemetry_info = client.ReadItem(doc_link,options)
        #print(win_telemetry_info)
        
    except:
        tc.track_trace('{} Failed TO FIND DOC {} Ingest Log in COSMOSDB, DOC_ID: {}'.format(LOG_MESSAGE_HEADER,doc_order,doc_id))
        tc.track_event(APP_INSIGHT_MAIN_ERROR_EVENT_NAME, {'MESSAGE': 'Failed TO FIND DOC Ingest Log in COSMOSDB','DOC_ID': doc_id },{ })
        
        tc.flush()
        print ('{} Failed TO FIND DOC Ingest Log in COSMOSDB, DOC_ID: {}'.format(LOG_MESSAGE_HEADER,doc_id))
        return
    
    if(win_telemetry_info is not None):
        ingest_updated=False
        for ingestion_info in win_telemetry_info["ingestions"]:
            try:
                if ingestion_info["status"]=='PENDING':
                    if ingestion_info["source_id"]==source_id:
                        tc.track_trace('{} Found Pending status, DOC_ID: {}, DOC_SOURCE_ID: {}, '.format(LOG_MESSAGE_HEADER,doc_id, source_id))
                        ingestion_info["status"]=status
                        ingestion_info["ingest_finish_time"]=datetime
                        ingestion_info["message"]=message

                        if status==SUCCESS_STATUS:
                            tc.track_trace('{} SUCCESS TO INGEST TO ADX, RUN_ID: {}, DOC_ORDER [{}], DOC_ID: {}, SOURCE_ID: {}'.format(LOG_MESSAGE_HEADER,run_id, doc_order, doc_id, source_id))
                            tc.track_event(APP_INSIGHT_INGEST_SUCCESS_EVENT_NAME, {'MESSAGE': 'SUCCESS TO Ingest ADX','DOC_ID': doc_id, 'SOURCE_ID':source_id },{"Total_RECORDS":ingestion_info["total_records"]})                  

                            tc.track_metric(APP_INSIGHT_INGEST_SUCCESS_COUNT_NAME, ingestion_info["total_records"])                  
                            tc.flush()
                            
                            publish_eventgrid(EVENT_GRID_ENDPOINT,EVENT_GRID_KEY,win_telemetry_info['vm_uuid'], win_telemetry_info['config_uuid'], win_telemetry_info['deploy_uuid'],file_path,ingestion_info['min_datatime'],ingestion_info['max_datatime'], ingestion_info['total_records'], win_telemetry_info['blob_account'], win_telemetry_info['blob_container_name']  )
            
                            tc.track_trace('{} Publish to Eventgrid, DOC_Order: {}  DOC_ID: {}, SOURCE_ID: {}'.format(LOG_MESSAGE_HEADER,doc_order, doc_id, source_id))
                            tc.flush()
                            
                        elif status==FAILURE_STATUS:
                            tc.track_trace('{} FAILED TO INGEST TO ADX,DOC_ORDER {},  DOC_ID: {}, SOURCE_ID {} '.format(LOG_MESSAGE_HEADER,doc_order, doc_id,source_id))
                            tc.track_metric(APP_INSIGHT_INGEST_FAILURE_COUNT_NAME, ingestion_info["total_records"])
                            tc.track_event(APP_INSIGHT_INGEST_FAILURE_EVENT_NAME, {'MESSAGE': 'Failed TO Ingest ADX','DOC_ID': doc_id , 'SOURCE_ID':source_id},{"Total_RECORDS":ingestion_info["total_records"]})
                            tc.flush()

                        client.UpsertItem(collection_link,win_telemetry_info,options)
                        ingest_updated=True
                        break
            except Exception:
                print(traceback.format_exc())
                print(sys.exc_info()[2])
 
        if(not ingest_updated):
            tc.track_trace('{} Failed TO FIND status=PENDING Ingest record with COSMOSDB DOC_ID: {}, SOURCE_ID:{}'.format(LOG_MESSAGE_HEADER,doc_id,source_id ))
            tc.track_event(APP_INSIGHT_MAIN_ERROR_EVENT_NAME, {'MESSAGE': 'Failed TO FIND status=PENDING Ingest record with COSMOSDB','DOC_ID': doc_id , 'SOURCE_ID':source_id},{ })
            tc.flush()
            print('{} Failed TO FIND status=PENDING Ingest record with COSMOSDB DOC_ID: {}'.format(LOG_MESSAGE_HEADER,doc_id))
    
def move_processed_file(blockblobservice, source_conatainer, source_filepath, target_container, target_filepath, tc):
    try:
        print("{} Start Move Processed Files".format(LOG_MESSAGE_HEADER))
        blob_url = blockblobservice.make_blob_url(source_conatainer, source_filepath)
        print("{} Blob URL : {}".format(LOG_MESSAGE_HEADER, blob_url))
        # blob_url:https://demostorage.blob.core.windows.net/image-container/pretty.jpg

        blockblobservice.copy_blob( target_container, target_filepath, blob_url)
        #for move the file use this line
        blockblobservice.delete_blob(source_conatainer, source_filepath)
    except Exception:
        print(traceback.format_exc())
        print(sys.exc_info()[2])

        errormsg="{} Failed TO Clean Ingested File. Source Container: {}, Source File path:  {}. Target Container {}, Target File path {}, traceback {}, execute info {}".format(LOG_MESSAGE_HEADER,source_conatainer, source_filepath,target_container, target_filepath,str(traceback.format_exc()),str(sys.exc_info()[2]))
        print (errormsg)
        
        tc.track_trace(errormsg)
        tc.track_event(APP_INSIGHT_MAIN_ERROR_EVENT_NAME, {'MESSAGE': errormsg,'Doc Path': source_filepath },{ })
        tc.flush()

def get_filename(filepath):
    path_secs=filepath.split('/')
    filename=path_secs[len(path_secs)-1]
    return filename


def get_doc_id(container_id,file_path):
    file_path=file_path[len(file_path)-min(len(file_path),200):]  # COSMOSDB Can only acceppet doc id length less than 255 characters
    return (container_id+'_'+file_path).replace('/','_').replace('\\','_').replace('?','_').replace('#','_')


def get_vm_uuid_from_filename(file_path):
    uuid_rex="([a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}){1}"
    pattern_regex = re.compile(uuid_rex)
    result = pattern_regex.findall(file_path)
    if len(result)>0:
        return result[0]
    else:
        print ("{} Couldn't find vm_uuid from file path {}".format("", file_path))
        return "COULD_NOT_FIND_VM_UUID"


def update_ADX_ingest_status(tc):

    KCSB_INGEST = KustoConnectionStringBuilder.with_aad_device_authentication(DATA_INGESTION_URI)
    KCSB_INGEST.authority_id = APP_AAD_TENANT_ID
    INGESTION_CLIENT = KustoIngestClient(KCSB_INGEST)
    qs = KustoIngestStatusQueues(INGESTION_CLIENT)

    run_id=(str(uuid.uuid4()))[31:].upper()
    MAX_BACKOFF = 8
    backoff = 1
    
    total_queue_success_messages=0
    while True:
    ################### NOTICE ####################
    # in order to get success status updates,
    # make sure ingestion properties set the
    # reportLevel=ReportLevel.FailuresAndSuccesses.
        if qs.success.is_empty() and qs.failure.is_empty():
            time.sleep(backoff)
            
            if backoff==1 and total_queue_success_messages!=0:
                print ("{} RUN_ID:{}  Processed {} message in this batch ".format(LOG_MESSAGE_HEADER,run_id, total_queue_success_messages))

            backoff = min(backoff * 2, MAX_BACKOFF)
            if(backoff<MAX_BACKOFF):
                #print("{} No new messages. backing off for {} seconds".format(LOG_MESSAGE_HEADER,backoff))
                continue
            if(backoff==MAX_BACKOFF):
                #print("{} Reach max waiting time {}, exit.".format(LOG_MESSAGE_HEADER,backoff))
                break

        backoff = 1

        success_messages = qs.success.pop(15)
        failure_messages = qs.failure.pop(15)

        total_success=0
        total_failure=0
        if success_messages is not None:
            if (len(success_messages)>0):
                tc.track_trace("{} Get {} success ingest messages ".format(LOG_MESSAGE_HEADER,str(len(success_messages))))  
                total_success=len(success_messages)
        if failure_messages is not None:
            if (len(failure_messages)>0):
                tc.track_trace("{} Get {} failure  ingest messages ".format(LOG_MESSAGE_HEADER,str(len(failure_messages))))  
                total_failure=len(failure_messages)
        tc.flush()
        total_queue_success_messages+=len(success_messages)
        count_success=0
        count_faulure=0
        for smsg in success_messages:
            file_path=get_file_path(smsg.IngestionSourcePath)
            container_name=get_container_name(smsg.IngestionSourcePath)
            count_success+=1
            log_msg="{} SUCCESS TO INGEST TO ADX <{}> -[{}/{}/{}] , Time: {}, vm_uuid: {}, source_id:{},  file path: {}".format(LOG_MESSAGE_HEADER,run_id,str(count_success), str(total_success), str(total_queue_success_messages),smsg.SucceededOn,get_vm_uuid_from_filename(file_path),smsg.IngestionSourceId, file_path)
            tc.track_trace(log_msg)            
            tc.track_event(APP_INSIGHT_INGEST_SUCCESS_EVENT_NAME, {'MESSAGE': 'SUCCESS TO Ingest ADX','file_path': file_path ,'source_id':smsg.IngestionSourceId },{})                                  
            tc.flush()
            update_COSMOS_status(COSMOS_CLIENT, file_path,smsg.SucceededOn,SUCCESS_STATUS,str(smsg),get_vm_uuid_from_filename(file_path),smsg.IngestionSourceId,container_name ,tc,count_success ,run_id )
   
            telemetry_block_blob_service = BlockBlobService(account_name=SOURCE_TELEMETRY_BLOB_ACCOUNT,   account_key=SOURCE_TELEMETRY_FILE_BLOB_KEY)

            target_file_path=''
            if (PROCESSED_TELEMETRY_FOLDER.endswith('/')):
                target_file_path=PROCESSED_TELEMETRY_FOLDER+file_path
            else :
                target_file_path=PROCESSED_TELEMETRY_FOLDER+'/'+file_path
                
            move_processed_file(telemetry_block_blob_service,container_name,file_path,container_name,target_file_path,tc)
            tc.track_trace('{} DONE ADX INGESTION PROCESS <{}> -[{}/{}/{}], File Moved to processed folder {} , vm_uuid: {}, file path: {}'.format(LOG_MESSAGE_HEADER,run_id,str(count_success), str(total_success), str(total_queue_success_messages),target_file_path,get_vm_uuid_from_filename(file_path),file_path))
            tc.track_event(APP_INSIGHT_INGEST_SUCCESS_EVENT_NAME, {'MESSAGE': 'DONE ADX INGESTION PROCESS','moved_file_path':target_file_path,'source_file_path':file_path },{})                                  
            tc.flush()
            #smsgjson=json.loads(smsg)
            #print (smsgjson['IngestionSourcePath'])
            #print (smsgjson['SucceededOn'])  
            print ("{} IngestionSourcePath: {}".format(LOG_MESSAGE_HEADER,smsg.IngestionSourcePath))
            print (smsg.SucceededOn)  
        for fmsg in failure_messages:
            container_name=get_container_name(fmsg.IngestionSourcePath)
            file_path=get_file_path(fmsg.IngestionSourcePath)
            count_faulure+=1
            log_msg="{} FAILED TO INGEST TO ADX <{}> -[{}/{}] , Time: {}, vm_uuid: {}, source_id:{}, container:{},  file path: {}, message: {}".format(LOG_MESSAGE_HEADER,run_id, str(count_faulure), str(total_failure),fmsg.FailedOn,get_vm_uuid_from_filename(file_path),fmsg.IngestionSourceId ,container_name,file_path,str(fmsg))
            tc.track_trace(log_msg)                              
            tc.track_event(APP_INSIGHT_INGEST_FAILURE_EVENT_NAME, {'MESSAGE': 'FAILED TO Ingest ADX','file_path': file_path, 'source_id':fmsg.IngestionSourceId },{})                                  
            tc.flush()
            update_COSMOS_status(COSMOS_CLIENT,file_path,fmsg.FailedOn,FAILURE_STATUS,str(fmsg),get_vm_uuid_from_filename(file_path),fmsg.IngestionSourceId,container_name,tc , count_faulure,run_id)

            
            
def publish_eventgrid(EVENT_GRID_ENDPOINT,EVENT_GRID_KEY,vm_uuid, config_uuid, deploy_uuid,file_path,min_unixtime,max_unixtime, total_records,storage_account,container_name ):
    
    credentials = TopicCredentials(EVENT_GRID_KEY )
    event_grid_client = EventGridClient(credentials)
    event_time=datetime.datetime.fromtimestamp(time.time())
    events_content=[{
            'id' : str(uuid.uuid1()),
            'subject' : "FINISH INGEST TO ADX",
            'data': {
                'vm_uuid': vm_uuid,
                'config_uuid':config_uuid,
                'deploy_uuid':deploy_uuid,
                'file_path':file_path,
                'min_unixtime':min_unixtime,
                'max_unixtime':max_unixtime,
                'total_records':total_records,
                'storage_account':storage_account,
                'container_name':container_name
                
            },
            'event_type': 'INGEST_ADX_FINSIHED',
            'event_time': event_time,
            'data_version': 1
        }]
    
    print ("{} Send to EventGrid with Content: {}".format(LOG_MESSAGE_HEADER,events_content))
    
    event_grid_client.publish_events(
        EVENT_GRID_ENDPOINT,
        events=events_content
    )         


def get_config_values():
    global COSMOS_URL,COSMOS_KEY,COSMOS_DATABASE,COSMOS_CONTAINER, COSMOS_CLIENT
    global SOURCE_TELEMETRY_BLOB_ACCOUNT,SOURCE_TELEMETRY_FILE_BLOB_KEY
    global APP_AAD_TENANT_ID,APP_CLIENT_ID,APP_CLIENT_SECRETS,DATA_INGESTION_URI
    global APP_INSIGHT_ID,APP_INSIGHT_INGEST_SUCCESS_COUNT_NAME,APP_INSIGHT_INGEST_FAILURE_COUNT_NAME
    global APP_INSIGHT_INGEST_SUCCESS_EVENT_NAME,APP_INSIGHT_INGEST_FAILURE_EVENT_NAME
    global LOG_MESSAGE_HEADER,PROCESS_PROGRAM_NAME
    # COSMOS CONFIG
    COSMOS_URL= os.getenv("COSMOS_URL",COSMOS_URL)
    COSMOS_KEY= os.getenv("COSMOS_KEY",COSMOS_KEY)
    COSMOS_DATABASE=os.getenv("COSMOS_DATABASE",COSMOS_DATABASE)
    COSMOS_CONTAINER=os.getenv("COSMOS_CONTAINER",COSMOS_CONTAINER)


    SOURCE_TELEMETRY_BLOB_ACCOUNT=os.getenv("SOURCE_TELEMETRY_BLOB_ACCOUNT",SOURCE_TELEMETRY_BLOB_ACCOUNT)
    SOURCE_TELEMETRY_FILE_BLOB_KEY=os.getenv("SOURCE_TELEMETRY_FILE_BLOB_KEY",SOURCE_TELEMETRY_FILE_BLOB_KEY)
 
    # ADX CONFIG
    APP_AAD_TENANT_ID = os.getenv("APP_AAD_TENANT_ID",APP_AAD_TENANT_ID)
    APP_CLIENT_ID = os.getenv("APP_CLIENT_ID",APP_CLIENT_ID)
    APP_CLIENT_SECRETS=os.getenv("APP_CLIENT_SECRETS",APP_CLIENT_SECRETS)
    #KUSTO_ENGINE_URI =os.environ("KUSTO_ENGINE_URI",KUSTO_ENGINE_URI)
    DATA_INGESTION_URI =os.getenv("DATA_INGESTION_URI",DATA_INGESTION_URI)

    APP_INSIGHT_ID=os.getenv("APP_INSIGHT_ID",APP_INSIGHT_ID)
    APP_INSIGHT_INGEST_SUCCESS_COUNT_NAME=os.getenv("INGEST_WIN_TELEMETRY_JSON_SUCCESS_COUNT",APP_INSIGHT_INGEST_SUCCESS_COUNT_NAME)
    APP_INSIGHT_INGEST_FAILURE_COUNT_NAME=os.getenv("INGEST_WIN_TELEMETRY_JSON_FAILURE_COUNT",APP_INSIGHT_INGEST_FAILURE_COUNT_NAME)
    APP_INSIGHT_INGEST_SUCCESS_EVENT_NAME=os.getenv("INGEST_WIN_TELEMETRY_JSON_SUCCESS",APP_INSIGHT_INGEST_SUCCESS_EVENT_NAME)
    APP_INSIGHT_INGEST_FAILURE_EVENT_NAME=os.getenv("INGEST_WIN_TELEMETRY_JSON_FAILURE",APP_INSIGHT_INGEST_FAILURE_EVENT_NAME)

    LOG_MESSAGE_HEADER=os.getenv("LOG_MESSAGE_HEADER",LOG_MESSAGE_HEADER)
    PROCESS_PROGRAM_NAME=os.getenv("PROCESS_PROGRAM_NAME",PROCESS_PROGRAM_NAME)
    COSMOS_CLIENT = cosmos_client.CosmosClient(COSMOS_URL, {'masterKey': COSMOS_KEY})

def main(mytimer: func.TimerRequest) -> None:

    get_config_values()

    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    tc = TelemetryClient(APP_INSIGHT_ID)
    tc.context.application.ver = '1.0'
    tc.context.properties["PROCESS_PROGRAM"]=PROCESS_PROGRAM_NAME
    tc.context.properties["PROCESS_START"]=time.time()
    tc.track_trace('{} Start to Check ADX Ingest Log'.format(LOG_MESSAGE_HEADER))
    tc.flush()    

    update_ADX_ingest_status(tc)

                
