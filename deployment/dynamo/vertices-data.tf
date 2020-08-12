resource "aws_dynamodb_table" "poli-vertex" {
  name = "PoliVertex"
  #billing_mode = "PAY_PER_REQUEST"
  billing_mode = "PROVISIONED"
  read_capacity = 5
  write_capacity = 500
  hash_key = "uid"
  range_key = "name"

  attribute {
    name = "uid"
    type = "N"
  }

  attribute {
    name = "name"
    type = "S"
  }

  tags = {
    Name = "poli"
    Environment = "dev"
  }
}
