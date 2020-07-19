#!/usr/bin/env bash
aws emr create-cluster \
--applications Name=Ganglia Name=Spark Name=Zeppelin Name=Hadoop Name=Hive \
--ec2-attributes '{"KeyName":"tap-in","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-430ef13b","EmrManagedSlaveSecurityGroup":"sg-0d55d1f6777b3bba0","EmrManagedMasterSecurityGroup":"sg-0f1b658f8b5cb94de"}' \
--release-label emr-5.29.0 \
--log-uri 's3n://big-time-tap-in-spark/logs/' \
--instance-groups file://small-instances.json \
--configurations file://config.json \
--ebs-root-volume-size 10 \
--service-role EMR_DefaultRole \
--enable-debugging \
--name 'We-Poli' \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region us-west-2 \
--profile tap-in