
FUNCTION="bigqueryImport"
PROJECT="my-project-name"
BUCKET="my-bucket-name"

gcloud config set project ${PROJECT}

gcloud functions deploy bigqueryImport \
    --runtime python37 \
    --trigger-resource ${BUCKET} \
    --trigger-event google.storage.object.finalize
