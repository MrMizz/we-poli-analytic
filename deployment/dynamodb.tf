resource "aws_dynamodb_table" "poli-vertex-name-autocomplete" {
  name = "PoliVertexNameAutoComplete${terraform.workspace}"
  billing_mode = "PAY_PER_REQUEST"
  #billing_mode = "PROVISIONED"
  #read_capacity = 5
  #write_capacity = 5000
  hash_key = "prefix"

  attribute {
    name = "prefix"
    type = "S"
  }

  tags = {
    Name = "poli"
    Environment = "dev"
  }
}

##resource "aws_dynamodb_table" "poli-traversals-page-sb1" {
##  name = "PoliTraversalsPageSB1${terraform.workspace}"
##  billing_mode = "PAY_PER_REQUEST"
##  #billing_mode = "PROVISIONED"
##  #read_capacity = 5
##  #write_capacity = 5000
##  hash_key = "vertex_id"
##  range_key = "page_num"
##
##  attribute {
##    name = "vertex_id"
##    type = "N"
##  }
##
##  attribute {
##    name = "page_num"
##    type = "N"
##  }
##
##  tags = {
##    Name = "poli"
##    Environment = "dev"
##  }
##}
##
##resource "aws_dynamodb_table" "poli-traversals-page-sb2" {
##  name = "PoliTraversalsPageSB2${terraform.workspace}"
##  billing_mode = "PAY_PER_REQUEST"
##  #billing_mode = "PROVISIONED"
##  #read_capacity = 5
##  #write_capacity = 5000
##  hash_key = "vertex_id"
##  range_key = "page_num"
##
##  attribute {
##    name = "vertex_id"
##    type = "N"
##  }
##
##  attribute {
##    name = "page_num"
##    type = "N"
##  }
##
##  tags = {
##    Name = "poli"
##    Environment = "dev"
##  }
##}
##
##resource "aws_dynamodb_table" "poli-traversals-page-sb3" {
##  name = "PoliTraversalsPageSB3${terraform.workspace}"
##  billing_mode = "PAY_PER_REQUEST"
##  #billing_mode = "PROVISIONED"
##  #read_capacity = 5
##  #write_capacity = 5000
##  hash_key = "vertex_id"
##  range_key = "page_num"
##
##  attribute {
##    name = "vertex_id"
##    type = "N"
##  }
##
##  attribute {
##    name = "page_num"
##    type = "N"
##  }
##
##  tags = {
##    Name = "poli"
##    Environment = "dev"
##  }
##}
##
##resource "aws_dynamodb_table" "poli-traversals-page-sb4" {
##  name = "PoliTraversalsPageSB4${terraform.workspace}"
##  billing_mode = "PAY_PER_REQUEST"
##  #billing_mode = "PROVISIONED"
##  #read_capacity = 5
##  #write_capacity = 5000
##  hash_key = "vertex_id"
##  range_key = "page_num"
##
##  attribute {
##    name = "vertex_id"
##    type = "N"
##  }
##
##  attribute {
##    name = "page_num"
##    type = "N"
##  }
##
##  tags = {
##    Name = "poli"
##    Environment = "dev"
##  }
##}
##
##resource "aws_dynamodb_table" "poli-traversals-page-sb5" {
##  name = "PoliTraversalsPageSB5${terraform.workspace}"
##  billing_mode = "PAY_PER_REQUEST"
##  #billing_mode = "PROVISIONED"
##  #read_capacity = 5
##  #write_capacity = 5000
##  hash_key = "vertex_id"
##  range_key = "page_num"
##
##  attribute {
##    name = "vertex_id"
##    type = "N"
##  }
##
##  attribute {
##    name = "page_num"
##    type = "N"
##  }
##
##  tags = {
##    Name = "poli"
##    Environment = "dev"
##  }
##}
##
##resource "aws_dynamodb_table" "poli-traversals-page-count" {
##  name = "PoliTraversalsPageCount${terraform.workspace}"
##  billing_mode = "PAY_PER_REQUEST"
##  #billing_mode = "PROVISIONED"
##  #read_capacity = 5
##  #write_capacity = 5000
##  hash_key = "vertex_id"
##
##  attribute {
##    name = "vertex_id"
##    type = "N"
##  }
##
##  tags = {
##    Name = "poli"
##    Environment = "dev"
##  }
##}
##
resource "aws_dynamodb_table" "poli-vertex" {
  name = "PoliVertex${terraform.workspace}"
  billing_mode = "PAY_PER_REQUEST"
  #billing_mode = "PROVISIONED"
  #read_capacity = 5
  #write_capacity = 5000
  hash_key = "uid"

  attribute {
    name = "uid"
    type = "N"
  }

  tags = {
    Name = "poli"
    Environment = "dev"
  }
}

resource "aws_dynamodb_table" "poli-edge" {
  name = "PoliEdge${terraform.workspace}"
  billing_mode = "PAY_PER_REQUEST"
  #billing_mode = "PROVISIONED"
  #read_capacity = 5
  #write_capacity = 5000
  hash_key = "src_id"
  range_key = "dst_id"

  attribute {
    name = "src_id"
    type = "N"
  }

  attribute {
    name = "dst_id"
    type = "N"
  }

  tags = {
    Name = "poli"
    Environment = "dev"
  }
}
