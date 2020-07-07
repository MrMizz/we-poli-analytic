resource "aws_neptune_cluster" "default_cluster" {
  cluster_identifier                    = "poli-dev"
  final_snapshot_identifier             = "poli-dev-snapshot"
  engine                                = "neptune"
  engine_version                        = "1.0.2.2"
  neptune_subnet_group_name             = "public-private"
  neptune_cluster_parameter_group_name  = "neptunedbclusterparametergr-7a5dym23nt8s"
  port                                  = "8182"
  vpc_security_group_ids                = ["sg-068cd3c93d6bcd44b"]
  storage_encrypted                     = false
  skip_final_snapshot                   = false
  iam_database_authentication_enabled   = false
  apply_immediately                     = true
}

resource "aws_neptune_cluster_instance" "default_instance" {
  count                        = 1
  identifier_prefix            = "poli-dev-instance"
  cluster_identifier           = aws_neptune_cluster.default_cluster.id
  engine                       = "neptune"
  engine_version               = "1.0.2.2"
  instance_class               = "db.t3.medium"
  availability_zone            = "us-west-2c"
  neptune_subnet_group_name    = "public-private"
  neptune_parameter_group_name = "default.neptune1"
  port                         = "8182"
  publicly_accessible          = false
  promotion_tier               = 1
  auto_minor_version_upgrade   = true
  apply_immediately            = true
}