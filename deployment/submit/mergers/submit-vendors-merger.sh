spark-submit \
--class in.tap.we.poli.analytic.Main \
target/scala-2.11/we-poli-analytic-assembly-1.0.0-SNAPSHOT.jar \
--step vendors-merger \
--in1 data/vendors/transformed/out \
--in1-format json \
--out1 data/vendors/merged/out \
--out1-format json