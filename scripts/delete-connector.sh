#!/usr/bin/env bash

set -e


CONNECTOR_NAME=click-csv-source-connector

curl -s -X DELETE http://localhost:8083/connectors/${CONNECTOR_NAME} | jq

curl -s http://localhost:8083/connectors | jq
