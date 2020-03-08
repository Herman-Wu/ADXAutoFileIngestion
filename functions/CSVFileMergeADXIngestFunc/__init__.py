import azure.functions as func

import logging

import pandas as pd
import time
import datetime
from io import StringIO
from pandas.core.indexes.api import _union_indexes
import json
from io import StringIO
import codecs
import re
import os,sys
import copy
import traceback

from azure.storage.blob import BlockBlobService

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

# COSMOS CONFIG
COSMOS_URL="https://[COSMOS Server].documents.azure.com:443/"
COSMOS_KEY=""
COSMOS_DATABASE='[COSMOS DATABASE]'
COSMOS_CONTAINER='[]'


# ADX CONFIG
APP_AAD_TENANT_ID = "[ADX Service Priciple TENANT ID]"
APP_CLIENT_ID = '[ADX Service Priciple Client ID]'
APP_CLIENT_SECRETS='[ADX Service Priciple Secrets]'
DATA_INGESTION_URI = "https://[KUSTO Ingestion ].kusto.windows.net:443;Federated Security=True;Application Client Id="+APP_CLIENT_ID+";Application Key="+APP_CLIENT_SECRETS

DESTINATION_TABLE = ""
DESTINATION_TABLE_COLUMN_MAPPING = ""


CONFIG_FILE_BLOB_ACCOUNT=""
CONFIG_FILE_CONTAINER = ""
CONFIG_FILE_TOKEN = ""


SOURCE_CSV_BLOB_ACCOUNT=''
SOURCE_CSV_BLOB_TOKEN=''
SOURCE_CSV_CONTAINER=""
SOURCE_CSV_BLOB_KEY=""

from applicationinsights import TelemetryClient

FILE_OUTPUT_FOLDER=""
vm_uuid=""
deploy_uuid=""
config_uuid=""

AGGREGATION_FILES_NUM=20  # Not Used right now
#MAX_FILESIZE=204800000
MAX_FILESIZE=102400000
#MAX_FILESIZE=51200000
#MAX_FILESIZE=10240000
CONFIG_FILE_BLOB_KEY=""

CONTAINER = ""
ACCOUNT_NAME = ""
SAS_TOKEN = ""

#    

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    metricspath = req.params.get('metricspath')
    forceinsert = req.params.get('forceinsert')


    if not metricspath:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            metricspath = req_body.get('metricspath')
            forceinsert =  req_body.get('forceinsert')

    if forceinsert is None:
        forceinsert=''

    if metricspath:
        scode=400
        msg='Something Wrong, this is Default Message'
        #print (metricspath)
        try:
            scode, msg=process(metricspath,forceinsert )
        except  Exception as e:
            error_class = e.__class__.__name__ 
            detail = e.args[0] 
            cl, exc, tb = sys.exc_info() 
            lastCallStack = traceback.extract_tb(tb)[-1] 
            fileName = lastCallStack[0]
            lineNum = lastCallStack[1] 
            funcName = lastCallStack[2] 
            errMsg = "File \"{}\", line {}, in {}: [{}] {}".format(fileName, lineNum, funcName, error_class, detail)
        
            print("Unexpected error:", sys.exc_info()[0])
            traceback.print_exc()

            msg= errMsg+traceback.format_exc()

            tc = TelemetryClient('')
            tc.context.application.ver = '1.0'
            tc.context.properties["PROCESS_PROGRAM"]="BATCH_CSV_V001a"
            tc.context.properties["DATA_FOLDER"]=metricspath
            tc.track_trace(msg)

            tc.flush()
            scode=500

        return func.HttpResponse(f"Proccessed  {metricspath}! "+msg,  status_code=scode)


    else:
        return func.HttpResponse(
             "Please pass a metricspath on the query string or in the request body",
             status_code=400
        )

def process(filesrootfolder,forceinsert ):

    # Create process id as identify of this process 
    process_id=time.time()

    tc = TelemetryClient('')

    tc.context.application.ver = '1.0'
    tc.context.properties["PROCESS_PROGRAM"]="BATCH_CSV_V001a"
    tc.context.properties["PROCESS_START"]=time.time()
    tc.context.properties["DATA_FOLDER"]=filesrootfolder
    tc.context.properties["PROCESS_ID"]=process_id

    tc.track_trace('STRAT RUN BATHCH INGEST  CSV DATA from folder '+filesrootfolder)
    tc.track_event('BATHCH_INGEST_CSV_START', { 'PROCESS_ID': process_id,'DATA_FOLDER': filesrootfolder  }, { })
    tc.flush()

    tc.flush()
    #print (vm_uuid,deploy_uuid,config_uuid)


    # Prepare COSMOS Link

    url=COSMOS_URL
    #key = os.environ['ACCOUNT_KEY']
    key=COSMOS_KEY
    client = cosmos_client.CosmosClient(url, {'masterKey': key})
    database_id=COSMOS_DATABASE
    container_id=COSMOS_CONTAINER

    database_link = 'dbs/' + database_id
    collection_link = database_link + '/colls/' + container_id


    doc_id=vm_uuid+'_'+config_uuid+'_'+deploy_uuid+'_Metric'
    doc_link = collection_link + '/docs/' + doc_id

    options = {}
    options['enableCrossPartitionQuery'] = True
    options['maxItemCount'] = 5
    options['partitionKey'] = vm_uuid

    proc_log_doc=None
    try:
        proc_log_doc = client.ReadItem(doc_link,options)
    except:
        print("New Process  Metric Doc")
        
    if(proc_log_doc is not None):
        print ("Find Existing  Metric Doc ")

        if str(forceinsert).lower()!='true':  # Stop Proccess if data is already been proccessed
            return 400, doc_id+" is already been processed"

    else: # New process log
        proc_log_doc={}
        proc_log_doc["PROCESSES"]=[]
        proc_log_doc["DOC_TYPE"]="PROCESS_METRIC"
        proc_log_doc["PROCESS_PROGRAM"]="BATCH_METRIC_CSV_V001a"
        proc_log_doc['id']=doc_id
        
    tc.track_event('BATHCH_INGEST_METRIC_CSV', { 'PROCESS_ID': process_id }, { 'DATA_FOLDER': filesrootfolder })
    #+'_'+config_uuid+'_'+deploy_uuid , { 'DATA_FOLDER': telemetriespath }
    tc.flush()
    proc_log_this={}
    proc_log_this["PROCESS_PROGRAM"]="BATCH_METRIC_CSV_V001a"
    proc_log_this["PROCESS_START"]=time.time()
    proc_log_this["DATA_FOLDER"]=filesrootfolder
    proc_log_this['id']=vm_uuid+'_'+config_uuid+'_'+deploy_uuid+'_'+str(process_id)

    error_files,merged_files, source_files= merge_rename_core_columns_CSV(vm_uuid,deploy_uuid,config_uuid,'defualt_metrics_csv_001A',0,SOURCE_CSV_CONTAINER,filesrootfolder,FILE_OUTPUT_FOLDER,process_id)

    # ToDo  ...
    proc_log_this["PROCESS_ID"]=process_id
    proc_log_this["ERROR_SOURCE_FILES_COUNT"]=len(error_files)
    proc_log_this["SOURCE_FILES_COUNT"]=len(source_files)

    tc.track_metric('BATHCH_INGEST_CSV_ERROR_SOURCE_FILES_COUNT', len(error_files))
    tc.track_metric('BATHCH_INGEST_CSV_ERROR_SOURCE_SOURCE_FILES_COUNT', len(source_files))
    tc.flush()

    # print(str(len(error_files)),'  ',str(len(merged_files)))

    proc_log_this["PROCESS_END"]=time.time()
    proc_log_this["STATUS"]="OK"

    proc_log_this["STATUS_MESSAGE"]=("It takes %s seconds to ingest  CSV file from Blob Storage") % (proc_log_this["PROCESS_END"] - proc_log_this["PROCESS_START"])

    proc_log_doc["PROCESSES"].append(proc_log_this)
    proc_log_doc['LATEST_UPDATE_TIMESTAMP']=time.time()

    # Update Process Log 
    client.UpsertItem(collection_link,proc_log_doc,options)

    tc.track_trace('END RUN BATHCH INGEST  METRIC CSV DATA from folder '+filesrootfolder)

    tc.track_event('BATHCH_INGEST_METRIC_CSV_END', { 'PROCESS_ID': process_id,'DATA_FOLDER': filesrootfolder  }, { 'DEFECT_FILES_COUNT':len(error_files),'MERGED_FILES_COUNT':len(merged_files),'SOURCE_FILES_COUNT':len(source_files)})
    tc.flush()



def get_uuids(filesrootfolder):

   #if  filesrootfolder.endswith('/'):
    #    filesrootfolder=filesrootfolder[0:len(filesrootfolder)-1]
    if  not filesrootfolder.endswith('/'):
        filesrootfolder=filesrootfolder+'/'

    config_file_name=""     
    config_file_path=filesrootfolder
    config_file_name_end=config_file_path[len(config_file_path)-36:len(config_file_path)-1]
    print(config_file_name_end)

    #configfile_block_blob_service = BlockBlobService(account_name=CONFIG_FILE_BLOB_ACCOUNT, sas_token=CONFIG_FILE_TOKEN)
    configfile_block_blob_service = BlockBlobService(account_name=CONFIG_FILE_BLOB_ACCOUNT, account_key=CONFIG_FILE_BLOB_KEY)
    blobs=configfile_block_blob_service.list_blobs(CONFIG_FILE_CONTAINER, prefix=config_file_path, delimiter="/")
    #print (len(list(blobs)))
    blobpaths=[]
    for blob in blobs:
        #print(blob.name)
        if(blob.name.lower().endswith((config_file_name_end+'.json').lower())):
            config_file_name=blob.name
            print(blob.name)
    
    if(len(config_file_name)>10):
        #print (config_file_name)
        configstring = configfile_block_blob_service.get_blob_to_text(CONFIG_FILE_CONTAINER, config_file_name, encoding='utf_16_le').content

        #print (configstring)

        decoded_data=codecs.decode(configstring.encode(), 'utf-8-sig')
        config_obj=json.loads(decoded_data)


        print("done")
    return vm_uuid,deploy_uuid,config_uuid

def insert_json_cosmos(jsondoc):
    url=COSMOS_URL
    #key = os.environ['ACCOUNT_KEY']
    key=COSMOS_KEY
    client = cosmos_client.CosmosClient(url, {'masterKey': key})
    database_id=COSMOS_DATABASE
    container_id=COSMOS_CONTAINER
    client.CreateItem("dbs/" + database_id + "/colls/" + container_id, jsondoc)

def merge_rename_core_columns_CSV(vm_uuid,deploy_uuid,config_uuid,schema_ver,inject_ver,container_name,filesrootfolder,fileoutputfolder, process_id):
    #block_blob_service = BlockBlobService(account_name=SOURCE_CSV_BLOB_ACCOUNT,  sas_token=SOURCE_CSV_BLOB_TOKEN)
    block_blob_service = BlockBlobService(account_name=SOURCE_CSV_BLOB_ACCOUNT,  account_key=SOURCE_CSV_BLOB_KEY)
    tc = TelemetryClient('')
    print("Start merge CSV ", vm_uuid,' ',deploy_uuid,' ',config_uuid)

    blobs = []
    marker = None
    while True:
        batch = block_blob_service.list_blobs(container_name, prefix=filesrootfolder)
        blobs.extend(batch)
        if not batch.next_marker:
            break
        marker = batch.next_marker
    i=0    
    blobpaths=[]
    for blob in blobs:
        blobpaths.append(blob.name)

    matchers = ['.csv']
    matching = [s for s in blobpaths if any(xs in s for xs in matchers)]
    
    
    mergelog={}
    mergelog["vm_uuid"]=vm_uuid

    mergelog["process_type"]="MERGE_METRIC_CSV"
    mergelog["DOC_TYPE"]="MERGE_METRIC_FILES_LOG"
    mergelog["file_folder"]=filesrootfolder
    mergelog["process_time"]=time.time()
    mergelog["files"]=[]
    mergelog["defect_files"]=[]
    
    a_mergelog=copy.deepcopy(mergelog)
    
    
    dfagg = pd.DataFrame(columns=[])
    
    mixagg=AGGREGATION_FILES_NUM
    aggcount=0
    aggcount_total=0
    aggoutcount=0
    aggsize=0
    
    error_files=[]
    merged_files=[]
    totoal_rows=0
    alldfs=[]
    outfilenamebase=fileoutputfolder+filesrootfolder+"_aggr_"
    t1=time.time()
    #print (outfilenamebase)
    source_col=['']
    target_col=[''] 
    
    tc.track_trace('Prepare to process '+str(len(matching))+'  Metric CSV files ')
    tc.flush()

    for fname in matching:
        #print(aggcount)
        
        head, tail = os.path.split(fname)
        
        aggcount+=1
        aggcount_total+=1

        blobstring = block_blob_service.get_blob_to_text(container_name, fname).content
        aggsize+=len(blobstring)

        #print('Prepare to merge '+str(aggcount_total)+' / '+str(len(matching)) +' Memeory '+str(aggsize)+' File Name: '+tail)
        #tc.track_trace('Prepare to merge '+tail)
        #tc.flush()


        try:  # Rread CSV And Try Processing 
        
            dfone = pd.read_csv(StringIO(blobstring))

            dfAll_cols=dfone.columns
            #colname0=dfAll_cols
            dfAll_newcols=[]

            pc_name=re.search(r'(\\{2}.*\\)(.*\\)', dfAll_cols[1]).group(1)

            for col in dfAll_cols:
                dfAll_newcols.append(col.replace(pc_name, '').replace('`', '').replace('\\','').replace(' ','').replace('/','').replace('.','').replace('-','').replace('%','').replace('(','').replace(')',''))

            dfAll_newcols[0]="Universal_datetime"

            # Rename all columns
            dfone.columns=dfAll_newcols

            alldfs.append(dfone)
            a_mergelog['files'].append(tail)

            #if (aggcount>=mixagg) or (aggcount_total==len(matching)):
            if (aggsize>MAX_FILESIZE) or (aggcount_total==len(matching)):
                if(aggcount_total==len(matching)):
                    print("Processing Final File")
                    tc.track_trace('Processing Final File')
                    tc.flush()

                alldfs.append(pd.DataFrame(columns=source_col))
                dfagg=pd.concat(alldfs, ignore_index=True)
                dfagg_out=dfagg[source_col]
                dfagg_out.columns=target_col
                dfagg_out['schema_ver']=schema_ver
                dfagg_out['inject_ver']=inject_ver
                output = dfagg_out.to_csv (index=False, encoding = "utf-8")
                outfile=outfilenamebase+str(aggoutcount)+".csv"
                block_blob_service.create_blob_from_text(container_name,outfile, output)
                print ("Output aggregated file to "+container_name, outfile+" Data Shape "+ str(dfagg.shape)+' uuid: '+str(vm_uuid)+str(deploy_uuid)+ str(config_uuid))
                totoal_rows+=dfagg_out.shape[0] 
               
                merged_files.append(outfile)


                a_mergelog['output_file']=outfile
                a_mergelog['merged_files_num']=len(a_mergelog['files'])
                a_mergelog['defect_files_num']=len(a_mergelog['defect_files'])

                # Insert Process Log to COSMOS DB 
                insert_json_cosmos(a_mergelog)
                a_mergelog=copy.deepcopy(mergelog)
                t2=time.time()

                print(("It takes %s seconds to merge "+str(aggcount)+" CSV Metrics") % (t2 - t1))
                aggoutcount+=1
                aggcount=0
                aggsize=0
                alldfs=[]
                t1=time.time()
                file_size=BlockBlobService.get_blob_properties(block_blob_service,container_name,outfile).properties.content_length
                print (outfile+"  File Size "+str(file_size))
                
                # Ingest to AXX
                ingest_to_ADX(outfile,file_size)
        except Exception as e :
            print ('Error While process '+fname)
            error_class = e.__class__.__name__ 
            detail = e.args[0] 
            cl, exc, tb = sys.exc_info()
            lastCallStack = traceback.extract_tb(tb)[-1] 
            fileName = lastCallStack[0] 
            lineNum = lastCallStack[1] 
            funcName = lastCallStack[2] 
            errMsg = "File \"{}\", line {}, in {}: [{}] {}".format(fileName, lineNum, funcName, error_class, detail)
        
            print("Unexpected error:", sys.exc_info()[0])
            traceback.print_exc()

            msg= errMsg+traceback.format_exc()

            tc = TelemetryClient('')
            tc.context.application.ver = '1.0'
            tc.context.properties["PROCESS_PROGRAM"]="BATCH_METRIC_CSV_V001a"
            tc.context.properties["DATA_FOLDER"]=metricspath
            tc.track_trace(msg)

            tc.flush()
            # print("Unexpected error:", sys.exc_info()[0])
            a_mergelog["defect_files"].append(tail)
            error_files.append(fname) # Add No-Well Formed JSON to error file
    print('Total Rows '+str(totoal_rows))

    tc.track_trace('Proccessed Rows: '+str(totoal_rows))
    tc.track_metric('BATHCH_INGEST_METRIC_CSV_TOTAL_ROWS', str(totoal_rows))
    tc.flush()
    return error_files,merged_files,matching


def ingest_to_ADX(filepath, filesize):
    KCSB_INGEST = KustoConnectionStringBuilder.with_aad_device_authentication(DATA_INGESTION_URI)
    KCSB_INGEST.authority_id = AAD_TENANT_ID

    KCSB_ENGINE = KustoConnectionStringBuilder.with_aad_device_authentication(URI)
    KCSB_ENGINE.authority_id = AAD_TENANT_ID
    
    INGESTION_CLIENT = KustoIngestClient(KCSB_INGEST)
    INGESTION_PROPERTIES  = IngestionProperties(database=DATABASE, table=DESTINATION_TABLE, dataFormat=DataFormat.CSV, mappingReference=DESTINATION_TABLE_COLUMN_MAPPING, additionalProperties={'ignoreFirstRecord': 'true'}, reportLevel=ReportLevel.FailuresAndSuccesses)
    BLOB_PATH = "https://" + SOURCE_CSV_BLOB_ACCOUNT + ".blob.core.windows.net/" + SOURCE_CSV_CONTAINER + "/" + filepath + SOURCE_CSV_BLOB_TOKEN

    BLOB_DESCRIPTOR = BlobDescriptor(BLOB_PATH, filesize)  # 10 is the raw size of the data in bytes
    INGESTION_CLIENT.ingest_from_blob(BLOB_DESCRIPTOR,ingestion_properties=INGESTION_PROPERTIES)

    print('Done queuing up ingestion with Azure Data Explorer '+filepath)