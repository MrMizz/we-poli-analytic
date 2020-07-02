spark-submit \
--master yarn \
--deploy-mode cluster \
--class in.tap.we.poli.analytic.Main \
s3://big-time-tap-in-spark/poli/jars/latest/we-poli-analytic-assembly-1.0.0-SNAPSHOT.jar \
--step vendors-transformer \
--in1 s3://big-time-tap-in-spark/poli/parsed/operating-expenditures/ \
--in1-format json \
--out1 s3://big-time-tap-in-spark/poli/transformed/vendors/2020-07-02-01/ \
--out1-format json