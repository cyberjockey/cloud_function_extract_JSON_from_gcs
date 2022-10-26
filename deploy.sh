#!/bin/sh

FUNCTION="bigqueryImport"
PROJECT="key-petal-364403"
BUCKET="kuelap_bucket_test"

# set the gcloud project
gcloud config set project ${PROJECT}

gcloud functions deploy bigqueryImport \
    --runtime python37 \
    --trigger-resource ${BUCKET} \
    --trigger-event google.storage.object.finalize
