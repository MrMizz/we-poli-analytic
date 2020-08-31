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

  attribute {
    name = "prefix"
    type = "S"
  }

  tags = {
    Name = "poli"
    Environment = "dev"
  }
}

# TODO: turn back on when ready write again. this will force replace.
#resource "aws_dynamodb_table" "poli-vertex" {
#  name = "PoliVertex2"
#  billing_mode = "PAY_PER_REQUEST"
#  #billing_mode = "PROVISIONED"
#  #read_capacity = 5
#  #write_capacity = 5000
#  hash_key = "uid"
#
#  attribute {
#    name = "uid"
#    type = "N"
#  }
#
#  tags = {
#    Name = "poli"
#    Environment = "dev"
#  }
#}