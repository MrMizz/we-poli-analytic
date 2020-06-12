#!/usr/bin/env bash

jarDate="latest"

sbt assembly
aws s3 cp target/scala-2.11/we-poli-analytic-assembly-1.0.0-SNAPSHOT.jar s3://big-time-tap-in-spark/poli/jars/$jarDate/ --profile tap-in
