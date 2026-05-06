#!/usr/bin/env bash
set -e

airflow db migrate

airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@mal.local 2>/dev/null || true

case "$1" in
  webserver)
    exec airflow webserver
    ;;
  scheduler)
    exec airflow scheduler
    ;;
  *)
    exec "$@"
    ;;
esac
