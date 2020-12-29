#!/usr/bin/env bash

CLUSTER="j-1HDY58M00W483"
RUN_DATE="2020-12-23-01"
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
--out1-format,json\
]

###################################################
### CONNECTORS
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
--in1-format,json,\
--out1,s3://big-time-tap-in-spark/poli/connector/auto/vendors/$RUN_DATE/,\
--out1-format,json\
]

###################################################
### MERGERS
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
--in1-format,json,\
--in2,s3://big-time-tap-in-spark/poli/connector/auto/vendors/$RUN_DATE/,\
--in2-format,json,\
--out1,s3://big-time-tap-in-spark/poli/merged/vendors/$RUN_DATE/,\
--out1-format,json\
]

## TODO
## 1) Validation Tranformer
## 2) Validation Connector
## 3) Connector Unify
## 4) Unique Vendor Flatten
## 5) Merger, again

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
--in1,s3://big-time-tap-in-spark/poli/merged/vendors/$RUN_DATE/,\
--in1-format,json,\
--out1,s3://big-time-tap-in-spark/poli/graph/edges/committee-to-vendor/$RUN_DATE/,\
--out1-format,json\
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
--in2-format,json,\
--out1,s3://big-time-tap-in-spark/poli/graph/vertices/committees/$RUN_DATE/,\
--out1-format,json\
]

aws emr add-steps --cluster-id $CLUSTER --profile tap-in \
--steps Type=spark,Name=VendorVertices,\
Args=[\
--deploy-mode,cluster,\
--conf,spark.app.name=VendorVertices,\
--class,in.tap.we.poli.analytic.Main,\
$JAR_PATH,\
--step,vendors-vertex,\
--in1,s3://big-time-tap-in-spark/poli/merged/vendors/$RUN_DATE/,\
--in1-format,json,\
--out1,s3://big-time-tap-in-spark/poli/graph/vertices/vendors/$RUN_DATE/,\
--out1-format,json\
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
--in1-format,json,\
--in2,s3://big-time-tap-in-spark/poli/graph/vertices/vendors/$RUN_DATE/,\
--in2-format,json,\
--out1,s3://big-time-tap-in-spark/poli/graph/vertices/union/$RUN_DATE/,\
--out1-format,json\
]
