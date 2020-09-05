#!/usr/bin/env bash

CLUSTER="j-3BZYK4LQOECGM"
RUN_DATE="2020-09-02-01"
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
$JAR_PATH,\
--step,dynamo-vertex-name,\
--in1,s3://big-time-tap-in-spark/poli/graph/vertices/union/2020-08-12-01/,\
--in1-format,json,\
--in2,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/2020-08-12-01/,\
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


#########################################################################
## Edge Data ############################################################
#########################################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoEdges,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoEdges,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-edge-data,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,json,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/edges/$RUN_DATE/,\
--out1-format,json\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoEdgesWriter,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoEdgesWriter,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-edge-data-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/edges/$RUN_DATE/,\
--in1-format,json,\
--out1,PoliEdge,\
--out1-format,no-op\
]

#########################################################################
## GRAPH TRAVERSALS #####################################################
#########################################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversals,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversals,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,json,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/$RUN_DATE/,\
--out1-format,json,\
--out2,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/$RUN_DATE/,\
--out2-format,json\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriter,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriter,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/$RUN_DATE/,\
--in1-format,json,\
--out1,PoliTraversalsPage,\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageCountWriter,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriter,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-count-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/$RUN_DATE/,\
--in1-format,json,\
--out1,PoliTraversalsPageCount,\
--out1-format,no-op\
]

