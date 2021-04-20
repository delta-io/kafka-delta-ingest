ENDPOINT="http://localstack:4566"

TEST_TABLES=(e2e_smoke_test write_ahead_log_test)

for t in ${TEST_TABLES[@]}; do
  echo "Dropping and creating $t in dynamodb"
  aws dynamodb delete-table --table-name $t --endpoint-url=$ENDPOINT --no-sign-request --region custom > /dev/null 2>&1
  aws dynamodb create-table --table-name $t --endpoint-url=$ENDPOINT --no-sign-request --region custom \
    --attribute-definitions \
      AttributeName=transaction_id,AttributeType=N \
    --key-schema \
      AttributeName=transaction_id,KeyType=HASH \
    --provisioned-throughput \
      ReadCapacityUnits=10,WriteCapacityUnits=10 > /dev/null
done

