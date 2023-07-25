#!/bin/bash

# This script is meant to run on the docker container on redcap-db-p03 and monitor a directory for new files
# if found, it should push the file to the google bucket and initiate an import into the database

# assumes /PROD_TABLE_BACKUP is mounted to the google container and you are running this in the /PROD_TABLE_BACKUP/dump_complete folder

BUCKET=redcap_dev_sql_dumps
INSTANCE=redcap-mysql
DESTDB=redcap_andy
BUCKETFOLDER=redcap_andy_import
OUTPUTFILE=$(date "+%Y-%m-%d_%H%M")_import.log

echo "----------------------------"
echo $(date -u) STARTING

i=0
while true
do
  cnt=$(ls -1q | grep .sql.gz$ | wc -l)

  if [ $cnt -eq 0 ]; then
    # no files to process - so sleep for a minute and check again
    echo "."
    sleep 60
  else
    for filename in $(ls | grep .sql.gz$) ; do
      ((i=i+1))
      echo "$(date -u) [$i] $filename: copying to $BUCKET/$BUCKETFOLDER" | tee -a $OUTPUTFILE
      gsutil cp $filename gs://$BUCKET/$BUCKETFOLDER/$filename
      # rename local file by appending a done suffix
      rm -f "$filename".done 2> /dev/null
      mv "$filename" "done/$filename"
      # start importing the file
      # check that no active processes are running before beginning import
      RUNNING_PROCESS_NAME=$(gcloud sql operations list --instance=${INSTANCE} | grep "RUNNING" | cut -d' ' -f1)
      until [[ -z "$RUNNING_PROCESS_NAME" ]]; do
         # we have a running process -- we must wait for it
         echo "$(date -u) [$i] $filename: process $RUNNING_PROCESS_NAME still running - waiting to complete..." | tee -a $OUTPUTFILE
         gcloud sql operations wait "$RUNNING_PROCESS_NAME" --timeout=1800 --verbosity="critical" 2>&1 | tee -a $OUTPUTFILE
         RUNNING_PROCESS_NAME=$(gcloud sql operations list --instance=${INSTANCE} | grep "RUNNING" | cut -d' ' -f1)
      done
      echo "$(date -u) [$i] $filename: Starting Import into $INSTANCE $DESTDB" | tee -a $OUTPUTFILE
      while true; do
        RESULT=$(gcloud sql import sql $INSTANCE "gs://$BUCKET/$BUCKETFOLDER/$filename" --database=$DESTDB --quiet 2>&1)
        echo "$RESULT" | tee -a $OUTPUTFILE
        if [[ $RESULT == *"longer than expected"* ]]; then
            echo "$(date -u) [$i] $filename: Timeout" | tee -a $OUTPUTFILE
            sleep 5
          elif [[ $RESULT == *"in progress"* ]]; then
            echo "$(date -u) [$i] $filename: Another operation in progress - waiting 10 sec to try again" | tee -a $OUTPUTFILE
            sleep 10
          elif [[ $RESULT == *"ERROR"* ]]; then
            echo "Unknown error" | tee -a $OUTPUTFILE
            sleep 60
            break;
          else
            # assume success
            break;
        fi
      done
      echo "---------------------------------" | tee -a $OUTPUTFILE
      # take a breath
      sleep 2
    done
    echo "Finished loop $i processing $cnt files" | tee -a $OUTPUTFILE
  fi
done

