#!/usr/bin/env bash

CLUSTER="j-3TS50JZG5KXUD"
RUN_DATE="2020-03-16-01"
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
--in1-format,parquet,\
--in2,s3://big-time-tap-in-spark/poli/connector/vendors/auto/$RUN_DATE/,\
--in2-format,parquet,\
--in3,s3://big-time-tap-in-spark/poli/merged/vendors/auto/$RUN_DATE/,\
--in3-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/id-res/features/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=FuzzyConnectorTraining,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=FuzzyConnectorTraining,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,fuzzy-connector-training,\
--in1,s3://big-time-tap-in-spark/poli/id-res/features/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/id-res/training/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=FuzzyPredictor,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=FuzzyPredictor,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,fuzzy-predictor,\
--in1,s3://big-time-tap-in-spark/poli/merged/vendors/auto/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/id-res/prediction/$RUN_DATE/,\
--out1-format,parquet\
]
