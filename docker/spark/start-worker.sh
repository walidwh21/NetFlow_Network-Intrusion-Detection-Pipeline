#!/bin/bash
exec /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker "$@"
