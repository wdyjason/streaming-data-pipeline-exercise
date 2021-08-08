#!/usr/bin/env bash

set -e

# https://docs.confluent.io/kafka-connect-spooldir/current/connectors/csv_source_connector.html

KEY_SCHEMA='{
      "type": "STRUCT",
      "name": "itemKey",
      "fieldSchemas": {
        "itemId": {
          "type": "STRING"
        }
      }
    }
'


#KEY_SCHEMA='{
#      "type": "String",
#      "name": "itemKey"
#    }
#'

KEY_SCHEMA_ESCAPED=$(echo "$KEY_SCHEMA" | tr -d '[:space:]' | sed -e 's/"/\\"/g')

VALUE_SCHEMA='{
      "type": "STRUCT",
      "name": "item",
      "fieldSchemas": {
        "itemId": {
          "type": "STRING"
        },
        "description": {
          "type": "STRING"
        }
      }
    }
'

VALUE_SCHEMA_ESCAPED=$(echo "$VALUE_SCHEMA" | tr -d '[:space:]' | sed -e 's/"/\\"/g')

PAYLOAD="
{
    \"name\": \"item-csv-source-connector\",
    \"config\": {
        \"connector.class\": \"com.github.jcustenborder.kafka.connect.spooldir.SpoolDirCsvSourceConnector\",
        \"tasks.max\": \"1\",
        \"topic\": \"item_v1\",
        \"input.path\": \"/data/raw/item_v1/\",
        \"input.file.pattern\": \"c.csv\",
        \"error.path\": \"/data/error/item_v1/\",
        \"finished.path\": \"/data/finished/item_v1\",
        \"csv.first.row.as.header\": true,
        \"key.schema\": \"${KEY_SCHEMA_ESCAPED}\",
        \"value.schema\": \"${VALUE_SCHEMA_ESCAPED}\"
    }
}
"

mkdir -p ./data/kafka-connect/raw/item_v1
mkdir -p ./data/kafka-connect/error/item_v1
mkdir -p ./data/kafka-connect/finished/item_v1

echo "${PAYLOAD}" | jq

curl -s http://localhost:8083/connectors -H 'Content-Type: application/json' -H 'Accept: application/json' \
  --data "${PAYLOAD}" | jq


curl -s http://localhost:8083/connectors | jq
