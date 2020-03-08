# An automatic File Ingestion System for Azure Data Explorer

This project is an automatic file ingestion system for [Azure Data Explorer (ADX or aka Kusto)](https://docs.microsoft.com/en-us/azure/data-explorer/data-explorer-overview). 

Azure Data Explorer is a fast and highly scalable data exploration service for log and telemetry data. To leverage it's powerful data analysis capablities the very basic step is you will need to ingest data into it. 

The design concept is when you have new data that need to ingest into ADX, you just need to upload the data into Azure Data Lake Gen2 or Azure Blob storage. The system will be triggered and ingests the data into a pre-defined table in ADX. It is useful when you need to continue to ingest log data from other systems 24&7. 

All the ingestion activities and results will be tracked and keep in Application Insight and COSMOS DB. The reason we keep the data in COSMOS DB is it provides an easier way to query and run statistic calculation for the ingestion status (eg. how much files are successfully ingested, which one is successfully ingested) then Application Insight. We also track if the same files (by name) are been ingested twice in COSMOS DB. If something goes wrong, the system will send alert to EventGrid and you will can have other system to get the notification (In our case, we send the alert message to Slack using LogicApp) 

The system leverages Azure Functions and EventGrid to trigger and connect each services. 

Here is an overview architecture: 

![Overview Architecture](https://github.com/Herman-Wu/ADXAutoFileIngestion/blob/master/doc/Architecture_s.jpg)

All the codes are implemented in python. 

**Note** 

The code is been extracted from a production project and I removed/changed the code specific related to the project. So some parts of the code will not working and need some fix to get it to work. But that should not be a lot of effort. 

I will try to find time to fix the broken part. 

