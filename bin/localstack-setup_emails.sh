#!/bin/bash

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-2
export ENDPOINT=http://localstack:4566
export AZURE_CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite:10000/devstoreaccount1;QueueEndpoint=http://azurite:10001/devstoreaccount1;"

function wait_for() {
  retries=10
  echo ">> running $2"
  set -x
  until eval $2 #> /dev/null 2>&1
  do
    if [ "$retries" -lt "0" ]; then
      echo "$1 is still offline after 10 retries";
      exit 1;
    fi
    echo "Waiting on $1 to start..."
    sleep 5
    retries=$((retries - 1))
  done
}

wait_for "Azurite" "az storage container list --connection-string ${AZURE_CONNECTION_STRING}"
az storage container create -n tests --connection-string ${AZURE_CONNECTION_STRING}
az storage blob upload-batch -d tests -s /data/emails -t block --overwrite --destination-path emails --connection-string ${AZURE_CONNECTION_STRING}

wait_for "S3" "aws s3api list-buckets --endpoint-url=$ENDPOINT"

echo "Create delta table in S3"
aws s3api create-bucket --bucket tests --endpoint-url=$ENDPOINT > /dev/null 2>&1
aws s3 sync /data/emails s3://tests/emails/ --delete --endpoint-url=$ENDPOINT

wait_for "DynamoDB" "aws dynamodb list-tables --endpoint-url=$ENDPOINT"

echo "Create delta-rs lock table in dynamo"
aws dynamodb delete-table --table-name locks --endpoint-url=$ENDPOINT > /dev/null 2>&1
aws dynamodb create-table \
  --endpoint-url=$ENDPOINT \
  --table-name locks \
  --attribute-definitions AttributeName=tablePath,AttributeType=S \
                          AttributeName=fileName,AttributeType=S \
  --key-schema AttributeName=tablePath,KeyType=HASH \
               AttributeName=fileName,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST
