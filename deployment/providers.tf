terraform {
  backend "s3" {
    bucket = "big-time-tap-in-tf-backends"
    key    = "we-poli-analytic"
    region = "us-west-2"
  }

  required_providers {
    aws = {
      source = "hashicorp/aws"
      version = "~> 3.40.0"
    }
  }

  required_version = "~>0.14.0"
}

provider "aws" {
  region     = "us-west-2"
}
