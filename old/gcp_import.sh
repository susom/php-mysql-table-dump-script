#!/bin/bash

# This was skipped in lieu of move_and_import

BUCKET=redcap_dev_sql_dumps
INSTANCE=redcap-dev-mysql
DESTDB=test2
BUCKETFOLDER=test2
OUTPUTFILE=import.log
#IMPORTEDFILE=imported.txt

## Try to load imported files
#if IMPORTED=$(gsutil cat gs://$BUCKET/$SRCFOLDER/imported.txt) ; then
#    printf "success"
#  else
#    printf "failed"
#fi

echo "$(date -u) STARTING SCRIPT: Importing $BUCKETFOLDER into $DESTDB of $INSTANCE (local time is $(date))" | tee $OUTPUTFILE

i=0
for filename in $(gsutil ls gs://$BUCKET/$BUCKETFOLDER | grep .sql.gz); do
  ((i=i+1))
  echo "$(date -u) [$i] Importing $filename" | tee $OUTPUTFILE
  gcloud sql import sql $INSTANCE "$filename" --database=$DESTDB --quiet | tee $OUTPUTFILE
  RESULT=$?
  if [ $RESULT -eq 0 ]; then
#    echo "$(date -u) complete" | tee $OUTPUTFILE
    sleep 2
  else
    echo "$(date -u) [$i] ERROR importing $filename" | tee $OUTPUTFILE
    break
  fi
done

echo "$(date -u) DONE" | tee $OUTPUTFILE
