#!/usr/bin/env bash

CLUSTER="j-132IOHJEUUZ0R"
RUN_DATE="2020-09-24-01"
JAR_PATH="s3://big-time-tap-in-spark/poli/jars/latest/we-poli-analytic-assembly-1.0.0-SNAPSHOT.jar"

###################################################
### ID RES ANALYTICS
###################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=FuzzyConnectorFeatures,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=FuzzyConnectorFeatures,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,fuzzy-connector-features,\
--in1,s3://big-time-tap-in-spark/poli/transformed/vendors/$RUN_DATE/,\
--in1-format,json,\
--in2,s3://big-time-tap-in-spark/poli/connector/vendors/$RUN_DATE/,\
--in2-format,json,\
--in3,s3://big-time-tap-in-spark/poli/merged/vendors/$RUN_DATE/,\
--in3-format,json,\
--out1,s3://big-time-tap-in-spark/poli/connector/fuzzy/features/$RUN_DATE/,\
--out1-format,json\
]