#!/bin/sh

PROJECT="from-template-leapp-reporting"
DB_TEMPLATE="mongodb-ephemeral"
REPORTING_API_REPO="https://github.com/fernflower/oamg-reporting-db-api"
REPORTING_KAFKA_LISTENER_REPO="https://github.com/fernflower/oamg-reporting"
ENV_FILE="./env"

oc delete --all all,secrets -n "$PROJECT"
oc new-app --template "$DB_TEMPLATE" -n "$PROJECT" --env-file "$ENV_FILE" \
       	   "$REPORTING_API_REPO" "$REPORTING_KAFKA_LISTENER_REPO"
oc expose svc/oamg-reporting-db-api
