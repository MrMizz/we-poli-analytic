#!/usr/bin/env bash

CLUSTER="j-3FC7ZR7OB15RL"
RUN_DATE="2020-08-12-01"
JAR_PATH="s3://big-time-tap-in-spark/poli/jars/latest/we-poli-analytic-assembly-1.0.0-SNAPSHOT.jar"

#########################################################################
## Vertex Name Autocomplete #############################################
#########################################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoVertexNames,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoVertexNames,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-vertex-name,\
--in1,s3://big-time-tap-in-spark/poli/graph/vertices/union/$RUN_DATE/,\
--in1-format,json,\
--in2,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in2-format,json,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/vertex-name-autocomplete/$RUN_DATE/,\
--out1-format,json\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoVertexNamesWriter,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoVertexNamesWriter,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-vertex-name-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/vertex-name-autocomplete/$RUN_DATE/,\
--in1-format,json,\
--out1,PoliVertexNameAutoComplete2,\
--out1-format,no-op\
]


#########################################################################
## Vertex Data ##########################################################
#########################################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoVertices,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoVertices,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-vertex-data-writer,\
--in1,s3://big-time-tap-in-spark/poli/graph/vertices/union/$RUN_DATE/,\
--in1-format,json,\
--out1,PoliVertex,\
--out1-format,no-op\
]
