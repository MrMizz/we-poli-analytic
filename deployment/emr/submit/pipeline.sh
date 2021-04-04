#!/usr/bin/env bash

CLUSTER="j-1H6UH4M19DKMH"
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
--steps Type=spark,Name=VendorsConnector,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=VendorsConnector,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,vendors-connector,\
--in1,s3://big-time-tap-in-spark/poli/transformed/vendors/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/connector/vendors/auto/$RUN_DATE/,\
--out1-format,parquet\
]

###################################################
### MERGERS (Auto)
###################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=VendorsMerger,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=VendorsMerger,\
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
--steps Type=spark,Name=UniqueVendorsConnector,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=UniqueVendorsConnector,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,unique-vendors-connector,\
--in1,s3://big-time-tap-in-spark/poli/id-res/transformed/vendors/$RUN_DATE/,\
--in1-format,parquet,\
--in2,s3://big-time-tap-in-spark/poli/connector/vendors/auto/$RUN_DATE/,\
--in2-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/connector/vendors/fuzzy/$RUN_DATE/,\
--out1-format,parquet\
]

###################################################
### MERGERS (Fuzzy)
###################################################
aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=UniqueVendorsMerger,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=UniqueVendorsMerger,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,unique-vendors-merger,\
--in1,s3://big-time-tap-in-spark/poli/merged/vendors/auto/$RUN_DATE/,\
--in1-format,parquet,\
--in2,s3://big-time-tap-in-spark/poli/connector/vendors/fuzzy/$RUN_DATE/,\
--in2-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/merged/vendors/fuzzy/$RUN_DATE/,\
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
--in1,s3://big-time-tap-in-spark/poli/merged/vendors/fuzzy/$RUN_DATE/,\
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
--in1,s3://big-time-tap-in-spark/poli/merged/vendors/fuzzy/$RUN_DATE/,\
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
--out1-format,parquet\
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
--out1-format,parquet\
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
--out1-format,parquet\
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
--out1-format,parquet\
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
--out1-format,parquet\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=DynamoGraphTraversalsPageCount,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=DynamoGraphTraversalsPage,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,dynamo-graph-traversal-page-count,\
--in1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page/sb1/$RUN_DATE/,\
--in1-format,parquet,\
--out1,s3://big-time-tap-in-spark/poli/dynamo/traversals/page-count/$RUN_DATE/,\
--out1-format,parquet\
]
