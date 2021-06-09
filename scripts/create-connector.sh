#!/usr/bin/env bash

set -e

# https://docs.confluent.io/kafka-connect-spooldir/current/connectors/csv_source_connector.html

KEY_SCHEMA='{
      "type": "STRING",
      "name": "click_key"
    }
'

KEY_SCHEMA_ESCAPED=$(echo "$KEY_SCHEMA" | tr -d '[:space:]' | sed -e 's/"/\\"/g')

VALUE_SCHEMA='{
      "type": "STRUCT",
      "name": "click",
      "fieldSchemas": {
        "itemId": {
          "type": "STRING"
        },
        "count": {
          "type": "INT64"
        },
        "eventTime": {
          "type": "INT64"
        }
      }
    }
'

VALUE_SCHEMA_ESCAPED=$(echo "$VALUE_SCHEMA" | tr -d '[:space:]' | sed -e 's/"/\\"/g')

PAYLOAD="
{
    \"name\": \"click-csv-source-connector\",
    \"config\": {
        \"connector.class\": \"com.github.jcustenborder.kafka.connect.spooldir.SpoolDirCsvSourceConnector\",
        \"tasks.max\": \"1\",
        \"topic\": \"click_1\",
        \"input.path\": \"/data/raw/click_1/\",
        \"input.file.pattern\": \".*csv\",
        \"error.path\": \"/data/error/click_1/\",
        \"finished.path\": \"/data/finished/click_1\",
        \"csv.first.row.as.header\": true,
        \"key.schema\": \"${KEY_SCHEMA_ESCAPED}\",
        \"value.schema\": \"${VALUE_SCHEMA_ESCAPED}\"
    }
}
"

mkdir -p ./data/kafka-connect/raw/click_1
mkdir -p ./data/kafka-connect/error/click_1
mkdir -p ./data/kafka-connect/finished/click_1

echo "${PAYLOAD}" | jq

curl -s http://localhost:8083/connectors -H 'Content-Type: application/json' -H 'Accept: application/json' \
  --data "${PAYLOAD}" | jq


curl -s http://localhost:8083/connectors | jq
