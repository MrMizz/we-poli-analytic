#!/usr/bin/env bash

CLUSTER="j-1HDY58M00W483"
RUN_DATE="2020-12-23-01"
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
--in2,s3://big-time-tap-in-spark/poli/connector/auto/vendors/$RUN_DATE/,\
--in2-format,json,\
--in3,s3://big-time-tap-in-spark/poli/merged/vendors/$RUN_DATE/,\
--in3-format,json,\
--out1,s3://big-time-tap-in-spark/poli/connector/fuzzy/features/$RUN_DATE/,\
--out1-format,json\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=FuzzyConnectorTraining,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=FuzzyConnectorTraining,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,fuzzy-connector-training,\
--in1,s3://big-time-tap-in-spark/poli/connector/fuzzy/features/$RUN_DATE/,\
--in1-format,json,\
--out1,s3://big-time-tap-in-spark/poli/connector/fuzzy/training/$RUN_DATE/,\
--out1-format,json\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=FuzzyPredictor,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=FuzzyPredictor,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,fuzzy-predictor,\
--in1,s3://big-time-tap-in-spark/poli/merged/vendors/$RUN_DATE/,\
--in1-format,json,\
--out1,s3://big-time-tap-in-spark/poli/connector/fuzzy/prediction/$RUN_DATE/,\
--out1-format,json\
]