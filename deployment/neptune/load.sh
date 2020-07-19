#!/usr/bin/env bash

curl -X POST \
    -H 'Content-Type: application/json' \
    https://poli-dev.cluster-ccagdblghqt8.us-west-2.neptune.amazonaws.com:8182/loader -d '
    {
      "source" : "s3://big-time-tap-in-spark/poli/graph/neptune/edges/2020-07-07-01",
      "format" : "csv",
      "iamRoleArn" : "arn:aws:iam::504084586672:role/NeptuneQuickStart-NeptuneSta-NeptuneLoadFromS3Role-WMFZNB4VZAZ1",
      "region" : "us-west-2",
      "failOnError" : "TRUE",
      "parallelism" : "HIGH",
      "updateSingleCardinalityProperties" : "TRUE",
      "queueRequest" : "TRUE"
    }'