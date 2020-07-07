spark-submit \
--master yarn \
--deploy-mode cluster \
--class in.tap.we.poli.analytic.Main \
s3://big-time-tap-in-spark/poli/jars/latest/we-poli-analytic-assembly-1.0.0-SNAPSHOT.jar \
--step vendors-merger \
--in1 s3://big-time-tap-in-spark/poli/transformed/vendors/2020-07-05-01/ \
--in1-format json \
--in2 s3://big-time-tap-in-spark/poli/connector/vendors/2020-07-05-01/ \
--in2-format json \
--out1 s3://big-time-tap-in-spark/poli/merged/vendors/2020-07-05-01/ \
--out1-format json