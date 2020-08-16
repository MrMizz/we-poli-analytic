variable "api_gateway_stage_name" {
  default = "v1"
}
variable "deployment_id" {
  ### increment to force deployment
  default = "2"
}

resource "aws_api_gateway_rest_api" "api" {
  name = "PoliData"
  description = "Poli Graph property data retrieval (no traversals)."
  endpoint_configuration {
    types = [
      "REGIONAL"
    ]
  }
}

## top level endpoint
resource "aws_api_gateway_resource" "poli" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  parent_id = aws_api_gateway_rest_api.api.root_resource_id
  path_part = "poli"
}

################################################################################
## ID GET REQUEST ##############################################################
################################################################################
resource "aws_api_gateway_resource" "id" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  parent_id = aws_api_gateway_resource.poli.id
  path_part = "id"
}

resource "aws_api_gateway_resource" "sub-id" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  parent_id = aws_api_gateway_resource.id.id
  path_part = "{id}"
}

resource "aws_api_gateway_method" "sub-id-method" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.sub-id.id
  http_method = "GET"
  authorization = "NONE"
}

# https://github.com/tillkuhn/yummy-aws/blob/master/terraform/terraform-apigw.tf
# great example for ddb tf
resource "aws_api_gateway_integration" "sub-id-integration" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.sub-id.id
  http_method = aws_api_gateway_method.sub-id-method.http_method
  integration_http_method = "POST"
  type = "AWS"
  uri = "arn:aws:apigateway:us-west-2:dynamodb:action/Query"
  credentials = "arn:aws:iam::504084586672:role/PoliGraphDataAccess"

  passthrough_behavior = "WHEN_NO_TEMPLATES"

  request_templates = {
    "application/json" = <<EOF
    {
      "TableName": "PoliVertex",
      "KeyConditionExpression": "uid = :key",
      "ExpressionAttributeValues": {
        ":key": {
          "N": "$input.params('id')"
        }
      }
    }
    EOF
  }
}

## Region Method: GET All regions - method response 200
resource "aws_api_gateway_method_response" "sub-id-response" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.sub-id.id
  http_method = aws_api_gateway_method.sub-id-method.http_method
  status_code = "200"
  response_parameters = {
    "method.response.header.Access-Control-Allow-Origin" = true
  }
}

## Mapping reference: https://docs.aws.amazon.com/de_de/apigateway/latest/developerguide/api-gateway-mapping-template-reference.html
## conditional? https://stackoverflow.com/questions/32511087/aws-api-gateway-how-do-i-make-querystring-parameters-optional-in-mapping-templa
resource "aws_api_gateway_integration_response" "sub-id-integration-response" {
  depends_on = [
    aws_api_gateway_integration.sub-id-integration
  ]
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.sub-id.id
  http_method = aws_api_gateway_method.sub-id-method.http_method
  status_code = aws_api_gateway_method_response.sub-id-response.status_code
  response_parameters = {
    "method.response.header.Access-Control-Allow-Origin" = "'*'"
  }
}

################################################################################
## PREFIX NAME AUTOCOMPLETE GET REQUEST ########################################
################################################################################
resource "aws_api_gateway_resource" "prefix" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  parent_id = aws_api_gateway_resource.poli.id
  path_part = "prefix"
}

resource "aws_api_gateway_resource" "sub-prefix" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  parent_id = aws_api_gateway_resource.prefix.id
  path_part = "{prefix}"
}

resource "aws_api_gateway_method" "sub-prefix-method" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.sub-prefix.id
  http_method = "GET"
  authorization = "NONE"
}

################################################################################
## DEPLOYMENT ##################################################################
################################################################################
## Deploy the Gateway Stage
## it seems you have to update the variable to actually force a deployment
resource "aws_api_gateway_deployment" "main" {
  depends_on = [
    aws_api_gateway_resource.poli
  ]
  rest_api_id = aws_api_gateway_rest_api.api.id
  stage_name = var.api_gateway_stage_name
  variables = {
    "answer" = var.deployment_id
  }
}
