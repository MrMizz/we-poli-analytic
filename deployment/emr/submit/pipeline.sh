#!/usr/bin/env bash

CLUSTER="j-IMRWBHKDUXSF"
RUN_DATE="latest"
JAR_PATH="s3://big-time-tap-in-spark/poli/jars/latest/we-poli-analytic-assembly-1.0.0-SNAPSHOT.jar"

###################################################
### TRANSFORMERS
###################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=VendorsTransformer,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=VendorsTransformer,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,vendors-transformer,\
--in1,s3://big-time-tap-in-spark/poli/parsed/operating-expenditures/,\
--in1-format,json,\
--out1,s3://big-time-tap-in-spark/poli/transformed/vendors/$RUN_DATE/,\
--out1-format,parquet\
]

###################################################
### CONNECTORS (Auto)
###################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=VendorsAutoConnector,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=VendorsAutoConnector,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,vendors-auto-connector,\
--in1,s3://big-time-tap-in-spark/poli/transformed/vendors/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/connector/vendors/auto/$RUN_DATE/,\
--out1-format,parquet\
]

###################################################
### ID RES VENDORS
###################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=IdResVendors,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=IdResVendors,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,id-res-vendors,\
--in1,s3://big-time-tap-in-spark/poli/transformed/vendors/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/id-res/transformed/vendors/$RUN_DATE/,\
--out1-format,parquet\
]

###################################################
### CONNECTORS (Fuzzy)
###################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=VendorsFuzzyConnector,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=VendorsFuzzyConnector,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,vendors-fuzzy-connector,\
--in1,s3://big-time-tap-in-spark/poli/id-res/transformed/vendors/$RUN_DATE/,\
--in1-format,parquet,\
--in2,s3://big-time-tap-in-spark/poli/connector/vendors/auto/$RUN_DATE/,\
--in2-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/connector/vendors/fuzzy/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=ConnctorsUnion,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=ConnctorsUnion,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,connectors-union,\
--in1,s3://big-time-tap-in-spark/poli/connector/vendors/auto/$RUN_DATE/,\
--in1-format,parquet,\
--in2,s3://big-time-tap-in-spark/poli/connector/vendors/fuzzy/$RUN_DATE/,\
--in2-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/connector/vendors/union/$RUN_DATE/,\
--out1-format,parquet\
]

###################################################
### MERGERS
###################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=VendorsAutoMerger,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=VendorsAutoMerger,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,vendors-merger,\
--in1,s3://big-time-tap-in-spark/poli/transformed/vendors/$RUN_DATE/,\
--in1-format,parquet,\
--in2,s3://big-time-tap-in-spark/poli/connector/vendors/auto/$RUN_DATE/,\
--in2-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/merged/vendors/auto/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=VendorsUnionMerger,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=VendorsUnionMerger,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,vendors-merger,\
--in1,s3://big-time-tap-in-spark/poli/transformed/vendors/$RUN_DATE/,\
--in1-format,parquet,\
--in2,s3://big-time-tap-in-spark/poli/connector/vendors/union/$RUN_DATE/,\
--in2-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/merged/vendors/union/$RUN_DATE/,\
--out1-format,parquet\
]

###################################################
### GRAPH
###################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=CommitteeToVendorEdges,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=CommitteeToVendorEdges,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,committee-to-vendor-edge,\
--in1,s3://big-time-tap-in-spark/poli/merged/vendors/union/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--out1-format,parquet\
]

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
--steps Type=spark,Name=CommitteeVertices,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=CommitteeVertices,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,committees-vertex,\
--in1,s3://big-time-tap-in-spark/poli/parsed/committees/,\
--in1-format,json,\
--in2,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in2-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/graph/vertices/committees/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=VendorVertices,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=VendorVertices,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,vendors-vertex,\
--in1,s3://big-time-tap-in-spark/poli/merged/vendors/union/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/graph/vertices/vendors/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=VertexUnion,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=VertexUnion,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,vertices-union,\
--in1,s3://big-time-tap-in-spark/poli/graph/vertices/committees/$RUN_DATE/,\
--in1-format,parquet,\
--in2,s3://big-time-tap-in-spark/poli/graph/vertices/vendors/$RUN_DATE/,\
--in2-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/graph/vertices/union/$RUN_DATE/,\
--out1-format,parquet\
]

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

############################################################################
## GRAPH TRAVERSALS INIT ###################################################
############################################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsInit,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsInit,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-init,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/init/$RUN_DATE/,\
--out1-format,parquet\
]


############################################################################
## GRAPH TRAVERSALS N1 #####################################################
############################################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN1N1SB1,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN1SB1,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n1-init,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/init/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n1/init/$RUN_DATE/,\
--out1-format,parquet\
]


aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN1N1SB1,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN1SB1,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n1-sb1,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n1/init/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n1/sb1/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN1SB2,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN1SB2,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n1-sb2,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n1/init/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n1/sb2/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN1SB3,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN1SB3,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n1-sb3,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n1/init/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n1/sb3/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN1SB4,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN1SB4,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n1-sb4,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n1/init/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n1/sb4/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN1SB5,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN1SB5,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n1-sb5,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n1/init/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n1/sb5/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageCountN1,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageCountN1,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-count,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n1/sb1/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/n1/$RUN_DATE/,\
--out1-format,parquet\
]

############################################################################
## GRAPH TRAVERSALS N2 #####################################################
############################################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN2N2SB1,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN2SB1,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n2-init,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/init/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n2/init/$RUN_DATE/,\
--out1-format,parquet\
]


aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN2N2SB1,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN2SB1,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n2-sb1,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n2/init/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n2/sb1/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN2SB2,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN2SB2,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n2-sb2,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n2/init/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n2/sb2/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN2SB3,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN2SB3,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n2-sb3,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n2/init/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n2/sb3/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN2SB4,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN2SB4,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n2-sb4,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n2/init/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n2/sb4/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN2SB5,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN2SB5,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n2-sb5,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n2/init/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n2/sb5/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageCountN2,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageCountN2,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-count,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n2/sb1/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/n2/$RUN_DATE/,\
--out1-format,parquet\
]


############################################################################
## GRAPH TRAVERSALS N3 #####################################################
############################################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN3SB1,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN3SB1,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n3-sb1,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n3/sb1/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN3SB2,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN3SB2,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n3-sb2,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n3/sb2/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN3SB3,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN3SB3,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n3-sb3,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n3/sb3/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN3SB4,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN3SB4,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n3-sb4,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n3/sb4/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN3SB5,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN3SB5,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n3-sb5,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n3/sb5/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageCountN3,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageCountN3,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-count,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n3/sb1/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/n3/$RUN_DATE/,\
--out1-format,parquet\
]


############################################################################
## GRAPH TRAVERSALS N4 #####################################################
############################################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN4SB1,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN4SB1,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n4-sb1,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n4/sb1/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN4SB2,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN4SB2,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n4-sb2,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n4/sb2/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN4SB3,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN4SB3,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n4-sb3,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n4/sb3/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN4SB4,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN4SB4,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n4-sb4,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n4/sb4/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN4SB5,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN4SB5,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n4-sb5,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n4/sb5/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageCountN4,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageCountN4,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-count,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n4/sb1/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/n4/$RUN_DATE/,\
--out1-format,parquet\
]


############################################################################
## GRAPH TRAVERSALS N5 #####################################################
############################################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN5SB1,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN5SB1,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n5-sb1,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n5/sb1/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN5SB2,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN5SB2,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n5-sb2,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n5/sb2/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN5SB3,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN5SB3,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n5-sb3,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n5/sb3/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN5SB4,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN5SB4,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n5-sb4,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n5/sb4/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsN5SB5,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsN5SB5,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-n5-sb5,\
--in1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n5/sb5/$RUN_DATE/,\
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageCountN5,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPageCountN5,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-count,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/n5/sb1/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/n5/$RUN_DATE/,\
--out1-format,parquet\
]
