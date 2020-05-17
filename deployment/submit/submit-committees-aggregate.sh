spark-submit \
--class in.tap.we.poli.analytic.Main \
target/scala-2.11/we-poli-analytic-assembly-1.0.0-SNAPSHOT.jar \
--step cmte-agg \
--in1 data/committees/in/ \
--in1-format json \
--out1 data/committees/aggregate/out \
--out1-format json
