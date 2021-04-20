ENDPOINT=http://0.0.0.0:4566

TEST_TABLES=(e2e_smoke_test write_ahead_log_test)

for t in ${TEST_TABLES[@]}; do
  echo "Scan for $t"
  aws dynamodb scan --table-name $t --endpoint-url=$ENDPOINT --consistent-read
done

