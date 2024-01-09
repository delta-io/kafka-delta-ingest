#!/bin/bash

WEB_REQUESTS_DIR=kafka-delta-ingest/tests/data/web_requests

find $WEB_REQUESTS_DIR/_delta_log -type f -not -name '00000000000000000000.json' -exec rm {} +
find $WEB_REQUESTS_DIR -type d -name 'date=*' -exec rm -rf {} +

