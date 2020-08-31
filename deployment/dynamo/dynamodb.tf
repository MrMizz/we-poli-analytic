resource "aws_dynamodb_table" "poli-vertex-name-autocomplete" {
  name = "PoliVertexNameAutoComplete"
  billing_mode = "PAY_PER_REQUEST"
  #billing_mode = "PROVISIONED"
  #read_capacity = 5
  #write_capacity = 25000
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

resource "aws_dynamodb_table" "poli-vertex-name-autocomplete2" {
  name = "PoliVertexNameAutoComplete2"
  #billing_mode = "PAY_PER_REQUEST"
  billing_mode = "PROVISIONED"
  read_capacity = 5
  write_capacity = 25000
  hash_key = "prefix"
  range_key = "is_committee"

  attribute {
    name = "prefix"
    type = "S"
  }

  attribute {
    name = "is_committee"
    type = "N"
  }

  tags = {
    Name = "poli"
    Environment = "dev"
  }
}