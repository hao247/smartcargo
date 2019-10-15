#!/bin/bash

S3CONFIGFILE=$PWD/config/s3_config.yaml
SCHEMAFILE=$PWD/config/schema.yaml
CREDENTFILE=$PWD/config/credentials.yaml


spark-submit --master spark://ip-10-0-0-8:7077 \
             --jars "/home/ubuntu/Downloads/postgresql-42.2.8.jar" \
             --driver-memory 4G \
             --executor-memory 4G \
             batch_processor/run_batch.py \
             $S3CONFIGFILE $SCHEMAFILE $CREDENTFILE
