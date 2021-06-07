#!/usr/bin/env bash

set -e

SCHEMA='{
      "type": "record",
      "name": "click",
      "fields": [
          {
              "name": "itemId",
              "type": "string"
          },
          {
              "name": "count",
              "type": "long"
          },
          {
              "name": "userId",
              "type": "long"
          },
          {
              "name": "eventTime",
              "type" : "long",
              "logicalType": "timestamp-millis"
          }
      ]
    }
'

SCHEMA_ESCAPED=$(echo "$SCHEMA" | tr -d '[:space:]' | sed -e 's/"/\\"/g')

PAYLOAD="
{
    \"schema\": \"${SCHEMA_ESCAPED}\"
}
"

echo "$PAYLOAD"
SUBJECT=click

echo "Test compatibility..."

curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "${PAYLOAD}" \
  "http://localhost:18081/compatibility/subjects/${SUBJECT}/versions/latest"


echo
echo "Migrate schema..."

curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
-H 'Accept: application/vnd.schemaregistry.v1+json, application/vnd.schemaregistry+json, application/json' \
  --data "${PAYLOAD}" \
  "http://localhost:18081/subjects/${SUBJECT}/versions"