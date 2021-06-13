terraform {
  backend "s3" {
    bucket = "big-time-tap-in-tf-backends"
    key    = "we-poli-analytic"
    region = "us-west-2"
  }
}

provider "aws" {
  region     = "us-west-2"
}
