#!/bin/bash

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-2
export ENDPOINT=http://localstack:4566

echo "Create delta table in S3"
aws s3api create-bucket --bucket tests --endpoint-url=$ENDPOINT #> /dev/null 2>&1
aws s3 sync /data/emails s3://tests/emails/ --delete --endpoint-url=$ENDPOINT

echo "Create delta-rs lock table in dynamo"
aws dynamodb delete-table --table-name locks --endpoint-url=$ENDPOINT #> /dev/null 2>&1
aws dynamodb create-table --table-name locks --endpoint-url=$ENDPOINT \
    --attribute-definitions \
        AttributeName=key,AttributeType=S \
    --key-schema \
        AttributeName=key,KeyType=HASH \
    --provisioned-throughput \
        ReadCapacityUnits=10,WriteCapacityUnits=10 #> /dev/null