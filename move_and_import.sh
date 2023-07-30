#!/bin/bash

# This script is meant to run on the docker container on redcap-db-p03 and monitor a directory for new files
# if found, it should push the file to the google bucket and initiate an import into the database

# assumes /PROD_TABLE_BACKUP is mounted to the google container and you are running this in the /PROD_TABLE_BACKUP/dump_complete folder

source move_and_import_config.sh

echo "----------------------------" | tee -a $OUTPUTFILE
echo $(date -u) STARTING : PID $$ | tee -a $OUTPUTFILE


function waitForProcess() {
  RUNNING_PROCESS_NAME=$(gcloud sql operations list --instance=${INSTANCE} | grep "RUNNING" | cut -d' ' -f1)
  if [[ -z "$RUNNING_PROCESS_NAME" ]]; then
    # Nothing running -- go ahead
    return 1
  else
    gcloud sql operations wait "$PROCESS_NAME" --timeout=7200 --verbosity="critical" 2>&1 | tee -a $OUTPUTFILE
    waitForProcess
  fi
}


function importBucket() {
  echo "$(date -u) [$filename] Starting Import into $INSTANCE $DESTDB" | tee -a $OUTPUTFILE
  RESULT=$(gcloud sql import sql $INSTANCE "gs://$BUCKET/$BUCKETFOLDER/$filename" --database=$DESTDB --quiet 2>&1)
  echo "$RESULT" | tee -a $OUTPUTFILE
  if [[ $RESULT == "Imported data"* ]]; then
    #success
    return 1
  elif [[ $RESULT == *"longer than expected"* ]]; then
    # hasn't finished
    waitForProcess
    # assume we end with success
  elif [[ $RESULT == *"in progress"* ]]; then
    echo "$(date -u) [$filename] Another operation in progress..." | tee -a $OUTPUTFILE
    waitForProcess
    importBucket
  elif [[ $RESULT == *"ERROR"* ]]; then
    echo "Unknown error" | tee -a $OUTPUTFILE
    return 0;
  else
    echo "Unknown response - $RESULT" | tee -a $OUTPUTFILE
    return 0;
  fi
}

while true
do
  cnt=$(ls -1q | grep .sql.gz$ | wc -l)

  if [ $cnt -eq 0 ]; then
    # no files to process - so sleep for a minute and check again
    echo "."
    sleep 30
  else
    filename=$(ls | grep .sql.gz$ | head -n 1)

    #step 1: copy file to bucket
    echo "$(date -u) [$filename] (1 of $cnt) Copying to $BUCKET/$BUCKETFOLDER" | tee -a $OUTPUTFILE
    gsutil cp $filename gs://$BUCKET/$BUCKETFOLDER/$filename

    #step 2: try import it
    # check that no active processes are running before beginning import
    waitForProcess
    IMPORT_RESULT=importBucket

    if [[ $IMPORT_RESULT ]]; then
      #success
      # rename local file by appending a done suffix
      # start importing the file
      echo "Finished $filename moving to done" | tee -a $OUTPUTFILE
      mv "$filename" "done/$filename"
    el
      #error
      echo "Finished $filename with error" | tee -a $OUTPUTFILE
      mv "$filename" "error/$filename"
    fi
    # take a breath
    sleep 1
  fi
done

