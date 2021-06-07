#!/usr/bin/env bash

set -e

SUBJECT=click

curl -X DELETE "http://localhost:18081/subjects/${SUBJECT}"

curl -X DELETE "http://localhost:18081/subjects/${SUBJECT}?permanent=true"