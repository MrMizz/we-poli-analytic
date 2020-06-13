spark-submit \
--class in.tap.we.poli.analytic.Main \
s3://big-time-tap-in-spark/poli/jars/latest/we-poli-analytic-assembly-1.0.0-SNAPSHOT.jar \
--step vendors-merger \
--in1 s3://big-time-tap-in-spark/poli/transformed/vendors/out/ \
--in1-format json \
--out1 s3://big-time-tap-in-spark/poli/merged/vendors/2020-06-12-04/ \
--out1-format json