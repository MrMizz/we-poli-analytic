#!/usr/bin/env bash

CLUSTER="j-3BZYK4LQOECGM"
RUN_DATE="latest"
JAR_PATH="s3://big-time-tap-in-spark/poli/jars/latest/we-poli-analytic-assembly-1.0.0-SNAPSHOT.jar"
INCR="Prod" ## increment when deploying to tf-workspace -> Prod2

#########################################################################
## Vertex Name Autocomplete #############################################
#########################################################################
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
--in1-format,parquet,\
--out1,PoliVertexNameAutoComplete"${INCR}",\
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
--in1-format,parquet,\
--out1,PoliVertex"${INCR}",\
--out1-format,no-op\
]


#########################################################################
## Edge Data ############################################################
#########################################################################
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
--in1-format,parquet,\
--out1,PoliEdge"${INCR}",\
--out1-format,no-op\
]


############################################################################
## GRAPH TRAVERSALS N1 #####################################################
############################################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN1SB1,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN1SB1,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n1/sb1/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN1SB1"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN1SB2,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN1SB2,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n1/sb2/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN1SB2"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN1SB3,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN1SB3,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n1/sb3/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN1SB3"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN1SB4,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN1SB4,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n1/sb4/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN1SB4"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN1SB5,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN1SB5,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n1/sb5/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN1SB5"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageCountWriterN1,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageCountWriterN1,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-count-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/n1/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageCountN1"${INCR}",\
--out1-format,no-op\
]


############################################################################
## GRAPH TRAVERSALS N2 #####################################################
############################################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN2SB1,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN2SB1,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n2/sb1/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN2SB1"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN2SB2,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN2SB2,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n2/sb2/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN2SB2"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN2SB3,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN2SB3,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n2/sb3/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN2SB3"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN2SB4,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN2SB4,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n2/sb4/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN2SB4"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN2SB5,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN2SB5,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n2/sb5/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN2SB5"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageCountWriterN2,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageCountWriterN2,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-count-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/n2/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageCountN2"${INCR}",\
--out1-format,no-op\
]


############################################################################
## GRAPH TRAVERSALS N3 #####################################################
############################################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN3SB1,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN3SB1,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n3/sb1/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN3SB1"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN3SB2,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN3SB2,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n3/sb2/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN3SB2"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN3SB3,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN3SB3,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n3/sb3/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN3SB3"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN3SB4,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN3SB4,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n3/sb4/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN3SB4"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN3SB5,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN3SB5,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n3/sb5/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN3SB5"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageCountWriterN3,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageCountWriterN3,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-count-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/n3/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageCountN3"${INCR}",\
--out1-format,no-op\
]


############################################################################
## GRAPH TRAVERSALS N4 #####################################################
############################################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN4SB1,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN4SB1,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n4/sb1/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN4SB1"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN4SB2,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN4SB2,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n4/sb2/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN4SB2"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN4SB3,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN4SB3,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n4/sb3/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN4SB3"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN4SB4,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN4SB4,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n4/sb4/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN4SB4"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN4SB5,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN4SB5,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n4/sb5/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN4SB5"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageCountWriterN4,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageCountWriterN4,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-count-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/n4/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageCountN4"${INCR}",\
--out1-format,no-op\
]

############################################################################
## GRAPH TRAVERSALS N5 #####################################################
############################################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN5SB1,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN5SB1,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n5/sb1/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN5SB1"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN5SB2,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN5SB2,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n5/sb2/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN5SB2"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN5SB3,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN5SB3,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n5/sb3/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN5SB3"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN5SB4,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN5SB4,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n5/sb4/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN5SB4"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterN5SB5,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterN5SB5,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n5/sb5/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageN5SB5"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageCountWriterN5,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageCountWriterN5,\
--class,in.tap.we.poli.analytic.Main,\
--packages,com.audienceproject:spark-dynamodb_2.11:1.0.4,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-count-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/n5/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageCountN5"${INCR}",\
--out1-format,no-op\
]
