#!/bin/bash

# This script is meant to run on the docker container on redcap-db-p03 and monitor a directory for new files
# if found, it should push the file to the google bucket and initiate an import into the database

# assumes /PROD_TABLE_BACKUP is mounted to the google container and you are running this in the /PROD_TABLE_BACKUP/dump_complete folder

source move_and_import_config.sh

echo "----------------------------" | tee -a $OUTPUTFILE
echo $(date -u) STARTING : PID $$ | tee -a $OUTPUTFILE


function getRunningProcess() {
  PROCESS=$(gcloud sql operations list --instance=${INSTANCE} --filter="status:RUNNING" | grep -Ewo '[[:xdigit:]]{8}(-[[:xdigit:]]{4}){3}-[[:xdigit:]]{12}')
  echo "DEBUG: RUNNING PROCESS: $PROCESS" | tee -a $OUTPUTFILE
}

function waitForProcess() {
  if [[ -z "$PROCESS" ]]; then
    # NOTHING TO WAIT FOR
    echo "DEBUG: No Process GUID Defined" | tee -a $OUTPUTFILE
  else
    echo "$(date -u) Waiting for $PROCESS to complete" | tee -a $OUTPUTFILE
    #PROCESS_RESULT=$(gcloud sql operations wait "$PROCESS" --timeout=unlimited --verbosity="critical" 2>&1)
    PROCESS_RESULT=$(gcloud sql operations wait "$PROCESS" --timeout=unlimited 2>&1)
    echo "$(date -u) $PROCESS_RESULT" | tee -a $OUTPUTFILE
#    sleep 5
  fi
}

function importBucket() {
  echo "$(date -u) [$filename] Importing into $INSTANCE $DESTDB" | tee -a $OUTPUTFILE
  IMPORT_RESULT=$(gcloud sql import sql $INSTANCE "gs://$BUCKET/$BUCKETFOLDER/$filename" --database=$DESTDB --async --quiet 2>&1)
#  echo "[RESULT]: $IMPORT_RESULT" | tee -a $OUTPUTFILE

  # parse out job id:
  #PROCESS=$(echo $IMPORT_RESULT | rev | cut -d "/" -f1 | rev)
  PROCESS=$(echo $IMPORT_RESULT | grep -Ewo '[[:xdigit:]]{8}(-[[:xdigit:]]{4}){3}-[[:xdigit:]]{12}')

#  echo "[PROCESS]: $PROCESS" | tee -a $OUTPUTFILE

  if [[ -z "$PROCESS" ]]; then
    # PROCESS NOT FOUND:
    echo "DEBUG: Unable to parse PROCESS from IMPORT_RESULT: $IMPORT_RESULT" | tee -a $OUTPUTFILE
    getRunningProcess
#    echo "DEBUG: PROCESS $PROCESS is running" | tee -a $OUTPUTFILE
    waitForProcess
#    echo "DEBUG: PROCESS $PROCESS is done" | tee -a $OUTPUTFILE
    return 0
  else
    # PROCESS FOUND

    waitForProcess
    # TODO: handle errors here?
    return 1
  fi
#  if [[ $RESULT == "Imported data"* ]]; then
#    #success
#    return 1
#  elif [[ $RESULT == *"longer than expected"* ]]; then
#    # hasn't finished
#    echo "DEBUG: Longer than expected" | tee -a $OUTPUTFILE
#    waitForProcess
#    # assume we end with success
#  elif [[ $RESULT == *"in progress"* ]]; then
#    echo "DEBUG: in_progress" | tee -a $OUTPUTFILE
#    echo "$(date -u) [$filename] Another operation in progress..." | tee -a $OUTPUTFILE
#    waitForProcess
#    importBucket
#  elif [[ $RESULT == *"ERROR"* ]]; then
#    echo "Unknown error" | tee -a $OUTPUTFILE
#    return 0;
#  else
#    echo "Unknown response - $RESULT" | tee -a $OUTPUTFILE
#    return 0;
#  fi

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
    echo "$(date -u) [$filename] ($cnt in queue) Copying to $BUCKET/$BUCKETFOLDER" | tee -a $OUTPUTFILE
    gsutil -q -o GSUtil:parallel_composite_upload_threshold=150M cp $filename gs://$BUCKET/$BUCKETFOLDER/$filename
    echo

    importBucket
    importBucketResult=$?

    if [[ $importBucketResult -eq 1 ]]; then
      #success
      echo "$(date -u) [$filename] Done!" | tee -a $OUTPUTFILE
      mv "$filename" "done/$filename"
    else
      #error
      echo "ERROR importing $filename - MOVED TO error DIRECTORY" | tee -a $OUTPUTFILE
      mv "$filename" "error/$filename"
      exit
    fi
    # take a breath
    sleep 1
  fi
done

