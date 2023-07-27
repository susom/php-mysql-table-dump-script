#!/bin/bash

# assumes /PROD_TABLE_BACKUP is mounted to the google container and you are running this in the /PROD_TABLE_BACKUP/dump_complete folder
#BUCKET=redcap_dev_sql_dumps
#INSTANCE=redcap-mysql
#DESTDB=redcap_andy
#BUCKETFOLDER=redcap_andy_import

# Production Migration 2023-07
BUCKET=redcap_som_prod_import
INSTANCE=redcap-prod
DESTDB=redcap_som_prod
BUCKETFOLDER=2023_07
OUTPUTFILE=$(date "+%Y-%m-%d_%H%M")_${INSTANCE}_${DESTDB}_import.log


