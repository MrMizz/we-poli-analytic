#!/usr/bin/env bash

CLUSTER="j-JPA3S9LPWHS5"
RUN_DATE="latest"
RUN_DATE2="latest"
JAR_PATH="s3://big-time-tap-in-spark/poli/jars/latest/we-poli-analytic-assembly-1.0.0-SNAPSHOT.jar"

###################################################
### ID RES ANALYTICS
###################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=IdResFeatures,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=IdResFeatures,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,id-res-features,\
--in1,s3://big-time-tap-in-spark/poli/id-res/transformed/vendors/$RUN_DATE/,\
--in1-format,parquet,\
--in2,s3://big-time-tap-in-spark/poli/connector/vendors/auto/$RUN_DATE/,\
--in2-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/id-res/features/$RUN_DATE2/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=IdResNameTraining,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=IdResNameTraining,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,id-res-name-training,\
--in1,s3://big-time-tap-in-spark/poli/id-res/features/$RUN_DATE2/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/id-res/training/name/$RUN_DATE2/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=IdResAddressTraining,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=IdResAddressTraining,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,id-res-address-training,\
--in1,s3://big-time-tap-in-spark/poli/id-res/features/$RUN_DATE2/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/id-res/training/address/$RUN_DATE2/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=IdResTransactionTraining,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=IdResTransactionTraining,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,id-res-transaction-training,\
--in1,s3://big-time-tap-in-spark/poli/id-res/features/$RUN_DATE2/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/id-res/training/transaction/$RUN_DATE2/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=IdResCompositeTraining,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=IdResCompositeTraining,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,id-res-composite-training,\
--in1,s3://big-time-tap-in-spark/poli/id-res/features/$RUN_DATE2/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/id-res/training/composite/$RUN_DATE2/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=IdResPredictor,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=IdResPredictor,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,id-res-predictor,\
--in1,s3://big-time-tap-in-spark/poli/id-res/transformed/vendors/$RUN_DATE/,\
--in1-format,parquet,\
--in2,s3://big-time-tap-in-spark/poli/connector/vendors/auto/$RUN_DATE/,\
--in2-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/id-res/prediction/$RUN_DATE2/,\
--out1-format,parquet\
]
