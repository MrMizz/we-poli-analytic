#!/usr/bin/env bash

CLUSTER="j-JPA3S9LPWHS5"
RUN_DATE="latest"
RUN_DATE2="dev-01"
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
--in1,s3://big-time-tap-in-spark/poli/id-res/transformed/vendors/$RUN_DATE/,\
--in1-format,parquet,\
--in2,s3://big-time-tap-in-spark/poli/connector/vendors/auto/$RUN_DATE/,\
--in2-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/id-res/features/$RUN_DATE2/,\
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
--in1,s3://big-time-tap-in-spark/poli/id-res/features/$RUN_DATE2/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/id-res/training/$RUN_DATE2/,\
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
--in1,s3://big-time-tap-in-spark/poli/id-res/transformed/vendors/$RUN_DATE/,\
--in1-format,parquet,\
--in2,s3://big-time-tap-in-spark/poli/connector/vendors/auto/$RUN_DATE/,\
--in2-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/id-res/prediction/$RUN_DATE2/,\
--out1-format,parquet\
]
