spark-submit \
--master yarn \
--deploy-mode cluster \
--class in.tap.we.poli.analytic.Main \
target/scala-2.11/we-poli-analytic-assembly-1.0.0-SNAPSHOT.jar \
--step vendors-transformer \
--in1 data/operating-expenditures/in/ \
--in1-format json \
--out1 data/vendors/transformed/out \
--out1-format json