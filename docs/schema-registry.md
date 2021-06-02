
```bash

SCHEMA='
{
    "type": "record",
    "name": "myrecord",
    "fields": [
        {
            "name": "count",
            "type": "long"
        },
        {
            "timestamp": "count",
            "type": "timestamp-millis"
        }
    ]
}
'
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "${SCHEMA}" \
  http://localhost:18081/subjects/click/versions
```