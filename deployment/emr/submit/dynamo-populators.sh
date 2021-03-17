#!/usr/bin/env bash

CLUSTER="j-3BZYK4LQOECGM"
RUN_DATE="2020-03-16-01"
JAR_PATH="s3://big-time-tap-in-spark/poli/jars/latest/we-poli-analytic-assembly-1.0.0-SNAPSHOT.jar"
INCR="Prod" ## increment when deploying to tf-workspace -> Prod2

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
--in1,s3://big-time-tap-in-spark/poli/graph/vertices/union/$RUN_DATE/,\
--in1-format,parquet,\
--in2,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in2-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/vertex-name-autocomplete/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoVertexNamesWriter,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoVertexNamesWriter,\
--class,in.tap.we.poli.analytic.Main,\
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
--steps Type=spark,Name=DynamoEdges,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoEdges,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-edge-data,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/edges/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoEdgesWriter,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoEdgesWriter,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-edge-data-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/edges/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliEdge"${INCR}",\
--out1-format,no-op\
]

#########################################################################
## GRAPH TRAVERSALS #####################################################
#########################################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsSB1,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsSB1,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-sb1,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/sb1/$RUN_DATE/,\
--out1-format,parquet,\
--out2,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/$RUN_DATE/,\
--out2-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsSB2,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsSB2,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-sb2,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/sb2/$RUN_DATE/,\
--out1-format,parquet,\
--out2,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/$RUN_DATE/,\
--out2-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsSB3,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsSB3,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-sb3,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/sb3/$RUN_DATE/,\
--out1-format,parquet,\
--out2,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/$RUN_DATE/,\
--out2-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsSB4,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsSB4,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-sb4,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/sb4/$RUN_DATE/,\
--out1-format,parquet,\
--out2,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/$RUN_DATE/,\
--out2-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsSB5,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsSB5,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-sb5,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/sb5/$RUN_DATE/,\
--out1-format,parquet,\
--out2,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/$RUN_DATE/,\
--out2-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterSB1,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterSB1,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/sb1/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageSB1"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterSB2,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterSB2,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/sb2/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageSB2"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterSB3,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterSB3,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/sb3/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageSB3"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterSB4,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterSB4,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/sb4/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageSB4"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageWriterSB5,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriterSB5,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/sb5/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageSB5"${INCR}",\
--out1-format,no-op\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageCountWriter,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageWriter,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-count-writer,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/$RUN_DATE/,\
--in1-format,parquet,\
--out1,PoliTraversalsPageCount"${INCR}",\
--out1-format,no-op\
]

