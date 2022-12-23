# Databricks notebook source
# MAGIC %pip install pyapacheatlas

# COMMAND ----------

from pyapacheatlas import *
from pyapacheatlas.auth import ServicePrincipalAuthentication
from pyapacheatlas.core import *

# COMMAND ----------

# create secrets or hardcode tenant id, client id & secret
TENANT_ID = dbutils.secrets.get(scope = "secrets", key = "DirectoryIdTo-AAD") 
CLIENT_ID =  dbutils.secrets.get(scope = "secrets", key = "ClientIdTo-Purview")
CLIENT_SECRET =  dbutils.secrets.get(scope = "KeyVault", key = "SecretTo-Purview")

# COMMAND ----------

def authenticatePurview(account_name, tenantId, clientId, clientSecret):
    oauth = ServicePrincipalAuthentication(
          tenant_id=tenantId,
          client_id=clientId,
          client_secret=clientSecret
      )
    client = PurviewClient(
          account_name = account_name,
          authentication=oauth
      )
    return client

purviewAccount = "purviewAccountName"

client = authenticatePurview(purviewAccount,TENANT_ID,  CLIENT_ID, CLIENT_SECRET) 


# COMMAND ----------

import json

def create_process_entity_json(processName):
    table_process_entity = EntityTypeDef(
      name=processName, 
      superTypes=["Process"],
      relationshipAttributeDefs=[
                  {
                      "name": "outputs",
                      "typeName": "array<DataSet>",
                      "isOptional": True,
                      "cardinality": "SET",
                      "valuesMinCount": 0,
                      "valuesMaxCount": 2147483647,
                      "isUnique": False,
                      "isIndexable": False,
                      "includeInNotification": False,
                      "relationshipTypeName": "process_dataset_outputs",
                      "isLegacyAttribute": True
                  },
                  {
                      "name": "inputs",
                      "typeName": "array<DataSet>",
                      "isOptional": True,
                      "cardinality": "SET",
                      "valuesMinCount": 0,
                      "valuesMaxCount": 2147483647,
                      "isUnique": False,
                      "isIndexable": False,
                      "includeInNotification": False,
                      "relationshipTypeName": "dataset_process_inputs",
                      "isLegacyAttribute": True
                  }
  ]
  )
    return table_process_entity.to_json()

def upload_json_process(client, json_entity, force_update ):
    upload_results = client.upload_typedefs(json_entity, force_update)
    return upload_results

# create process for each type of process in ETL
json_entity = create_process_entity_json("SQL Warehouse Transformation")
upload_results = upload_json_process(client, json_entity, True)
print(json.dumps(upload_results, indent=2))

# COMMAND ----------

# retrieve the type of entity from purview
def getEntityTypeOfGuid(guid):
    entity = client.get_entity(guid = guid)
    return entity['entities'][0]['typeName']
  
type = getEntityTypeOfGuid('3f703aa4-4da6-4d64-92f8-44f6f6f60000')#('fcd215d5-1a19-4367-ae1e-e211c33058af')
type

# COMMAND ----------

import pandas as pd
  
# dictionary with list object in values
details = {
    'SourceEntity' : ['payroll', 'employee', 'payroll', 'employee', 'Payroll', 'Employee'],
    'SourceEntityStage': ['RAW', 'RAW', 'BASE', 'BASE', 'BASE WAREHOUSE', 'BASE WAREHOUSE'],
    'TargetEntity' : ['payroll', 'employee', 'payroll', 'employee', 'DimEmployee', 'DimEmployee'],
    'TargetEntityStage': ['BASE', 'BASE', 'BASE WAREHOUSE', 'BASE WAREHOUSE', 'WAREHOUSE', 'WAREHOUSE'],
    'SourceLocation' : ['RAW/sourcesystem1', 'RAW/sourcesystem2','BASE/sourcesystem1', 'BASE/sourcesystem2', 'BASE',	'BASE'],
    'SourcePattern': ['payroll.csv', 'employee.csv', 'payroll.parquet', 'employee.parquet', 'SourceSystem1_Payroll', 'SourceSystem2_Employee'],
    'TargetLocation': ['BASE/sourcesystem1', 'BASE/sourcesystem2', 'BASE', 'BASE', 'WAREHOUSE', 'WAREHOUSE'],
    'TargetPattern': ['payroll.parquet', 'employee.parquet', 'SourceSystem1_Payroll', 'SourceSystem2_Employee', 'DimEmployee', 'DimEmployee'],
    'Dependency' : ['RAW TO BASE DATALAKE Payroll', 'RAW TO BASE DATALAKE Employee', 'BASE DATALAKE TO BASE WAREHOUSE Payroll', 'BASE DATALAKE TO BASE WAREHOUSE Employee', 'BASE WAREHOUSE TO DIM Employee', 'BASE WAREHOUSE TO DIM Employee']
}
  
# creating a Dataframe object 
df = pd.DataFrame(details)
  
df

# COMMAND ----------

DependencyGroupCodes = df.Dependency.unique()
DependencyGroupCodes

# COMMAND ----------

# come back here? 
# NOT USING THIS 
processTypeDict = { 
  'BASE': {'Process' : 'Databricks notebook validate schema', 'SourceType' : 'azure_datalake_gen2_resource_set' , 'TargetType': 'azure_datalake_gen2_resource_set', 'ProcessNameStart' : 'RAW'},
  'BASE WAREHOUSE': {'Process' : 'SQL Import Stored Procedure', 'SourceType' : 'azure_datalake_gen2_resource_set' , 'TargetType': 'azure_sql_dw_table', 'ProcessNameStart' : 'BASE DATALAKE'},
  'WAREHOUSE': {'Process' : 'SQL Warehouse Transformation', 'SourceType' : 'azure_sql_dw_table' , 'TargetType': 'azure_sql_dw_table', 'ProcessNameStart' : 'BASE WAREHOUSE'} 
}

# COMMAND ----------

# Create secrets or hardcode source roots
DatalakeRoot = dbutils.secrets.get(scope = "secrets", key = "DataLakeRootUrl") 
AzureSqlRoot = dbutils.secrets.get(scope = "secrets", key = "AzureSqlRoot") 

# COMMAND ----------

def createEntityList(datawarehouseQueryPandasDF):
    entitieslist = []
    for index, row in datawarehouseQueryPandasDF.iterrows():
        entitieslist.append(row.to_dict())
    return entitieslist

# can be multiple sources for one target so returns a list
def createSources(entitieslist):
    sources = []
    for entity in entitieslist:
        if (entity.get("SourceEntityStage") in ["RAW", "BASE"]):
            sources.append(DatalakeRoot + str(entity.get("SourceLocation")) + "/{Year}/{Month}/{Day}/" +  str(entity.get("SourcePattern")))
        if (entity.get("SourceEntityStage") in ["BASE WAREHOUSE", "WAREHOUSE"]):
            sources.append(AzureSqlRoot + str(entity.get("SourceLocation")) + "/" + str(entity.get("SourcePattern")) )
    return sources

# can be multiple sources for one target, hence take first item in array since same target for one dependency code 
def createTarget(entitieslist):
    if (entitieslist[0].get("TargetEntityStage") in ["RAW", "BASE"]):
          target = DatalakeRoot + str(entitieslist[0].get("TargetLocation")) + "/{Year}/{Month}/{Day}/" +  str(entitieslist[0].get("TargetPattern"))
    if (entitieslist[0].get("TargetEntityStage") in ["BASE WAREHOUSE", "WAREHOUSE"]):
            target = AzureSqlRoot + str(entitieslist[0].get("TargetLocation")) + "/" + str(entitieslist[0].get("TargetPattern"))
    return target

# can be multiple names  but must be of the same types. Code will fail if sources e.g. views & also tables 
def getEntity(client, entityName, entityType):
    targetEntities = client.get_entity(
              qualifiedName=entityName,
              typeName=entityType      
          )
    return targetEntities

def createProcessInputs(sourceEntities):
        inputs = []
        for sourceEntity in sourceEntities.values():
              if len(sourceEntity) > 0:
                for sourceEntityListItem in sourceEntity:
                      if type(sourceEntityListItem) is dict:     
                        inputs.append(AtlasEntity( #input entity
                          name            = sourceEntityListItem["attributes"]["name"],
                          typeName        = sourceEntityListItem["typeName"],
                          qualified_name  = sourceEntityListItem["attributes"]["qualifiedName"],
                          guid            = sourceEntityListItem["guid"]
                      ))
            inputs_json = []
        for input_entity in inputs:
            inputs_json.append(input_entity.to_json(minimum=True)) ## add the atlas entity 
        return inputs_json

# create process linking inputs to outputs 
def createProcess(inputs_json, outputs_json, processType, dep, description):
    process = AtlasProcess( 
                  name=dep,
                  typeName= processType, 
                  qualified_name=dep,
                  inputs=inputs_json,
                  outputs=outputs_json,
                  guid=-1,
                  attributes = {'description' : description}
    )  
    return process


# update the names of items
def updateAttribute(client, typeName, attributeName, qualifiedName, newSourceName):
    try:
        client.partial_update_entity(typeName = typeName, qualifiedName = qualifiedName, attributes={attributeName:newSourceName})  
        print('Update {0} Success for {1}'.format(attributeName, qualifiedName))
    except:
        print('Update {0} Fail for {1}'.format(attributeName, qualifiedName))
        
def uploadEntities(client, batch):
    if batch is None: 
        return None 
    else:
        result = client.upload_entities(batch=[batch])
        return result

# COMMAND ----------

# RAW TO BASE, also run for employee
sourceEntityType = 'azure_datalake_gen2_resource_set'
targetEntityType = 'azure_datalake_gen2_resource_set'
purviewProcessName = 'Databricks notebook validate schema'

dep = 'RAW TO BASE DATALAKE Payroll' # hard code for one process
dfFiltered = df[df['Dependency'] == dep] # filter dataframe to one process

entitieslist = createEntityList(dfFiltered) # create entity list

sources = createSources(entitieslist) # return list of source qualified names 
target = createTarget(entitieslist)   # return the target qualified name

sourceEntities = getEntity(client, sources, sourceEntityType) # get the source entities from Purview
targetEntities = getEntity(client, target, targetEntityType)  # get the target entity from Purview 

inputs_json = createProcessInputs(sourceEntities) # list of json inputs for the process
outputs_json = createProcessInputs(targetEntities) # list of json output for the process (length 1)

process = createProcess(inputs_json, outputs_json,purviewProcessName, dep, 'dummy description') # create the AtlasProcess object
results = client.upload_entities(batch=[process])   # upload the process to Purview

#updateAttribute(client, sourceEntityType, 'name', sources, 'Payroll RAW')    

# COMMAND ----------

# BASE base warehouse, also run for employee
sourceEntityType = 'azure_datalake_gen2_resource_set'
targetEntityType = 'azure_sql_table'
purviewProcessName = 'SQL Import Stored Procedure' 
#print(DependencyGroupCodes)
dep = 'BASE DATALAKE TO BASE WAREHOUSE Payroll'
dfFiltered = df[df['ProcessName'] == dep]#'RAW TO BASE Payroll']    
#print(dfFiltered)
entitieslist = createEntityList(dfFiltered)
sources = createSources(entitieslist)
target = createTarget(entitieslist)
#print(target)
sourceEntities = getEntity(client, sources, sourceEntityType)
print(sourceEntities)
targetEntities = getEntity(client, target, targetEntityType)
print(targetEntities)
inputs_json = createProcessInputs(sourceEntities)
outputs_json = createProcessInputs(targetEntities) 
process = createProcess(inputs_json, outputs_json,purviewProcessName, dep, 'dummy description')
results = uploadEntities(client, process)   
#updateAttribute(client, sourceEntityType, 'name', sources, 'Employee RAW')    

# COMMAND ----------

# DimEmployee
sourceEntityType = 'azure_sql_table'
targetEntityType = 'azure_sql_table'
purviewProcessName = 'SQL Warehouse Transformation' 
#print(DependencyGroupCodes)
dep = 'BASE WAREHOUSE TO DIM Employee'
dfFiltered = df[df['ProcessName'] == dep]#'RAW TO BASE Payroll'] 
#display(dfFiltered)
entitieslist = createEntityList(dfFiltered)
#print(entitieslist)
sources = createSources(entitieslist)
print(sources)
target = createTarget(entitieslist)
#print(target)
sourceEntities = getEntity(client, sources, sourceEntityType)
#print(sourceEntities)
targetEntities = getEntity(client, target, targetEntityType)
#print(targetEntities)
inputs_json = createProcessInputs(sourceEntities)
outputs_json = createProcessInputs(targetEntities) 
process = createProcess(inputs_json, outputs_json,purviewProcessName, dep, 'dummy description')
results = uploadEntities(client, process)   
#updateAttribute(client, sourceEntityType, 'name', sources, 'Employee RAW')    
