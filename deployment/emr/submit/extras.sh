#!/usr/bin/env bash

CLUSTER="j-132IOHJEUUZ0R"
RUN_DATE="2020-09-24-01"
JAR_PATH="s3://big-time-tap-in-spark/poli/jars/latest/we-poli-analytic-assembly-1.0.0-SNAPSHOT.jar"

###################################################
### ID RES ANALYTICS
###################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=VendorComparison,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=VendorComparison,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,vendors-comparison,\
--in1,s3://big-time-tap-in-spark/poli/merged/vendors/$RUN_DATE/,\
--in1-format,json,\
--out1,s3://big-time-tap-in-spark/poli/extra/vendors-comparison/$RUN_DATE/,\
--out1-format,json\
]