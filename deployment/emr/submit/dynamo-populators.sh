#!/usr/bin/env bash

CLUSTER="j-2LOLVR9OWYR3S"
RUN_DATE="2020-08-12-01"
JAR_PATH="s3://big-time-tap-in-spark/poli/jars/latest/we-poli-analytic-assembly-1.0.0-SNAPSHOT.jar"

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoVertices,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoVertices,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-vertex,\
--in1,s3://big-time-tap-in-spark/poli/graph/vertices/union/$RUN_DATE/,\
--in1-format,json,\
--out1,PoliVertex,\
--out1-format,no-op\
]
