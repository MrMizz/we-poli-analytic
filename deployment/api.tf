variable "deployment_id" {
  ### increment to force deployment
  default = "1"
}

resource "aws_api_gateway_rest_api" "api" {
  name = "PoliData${title(terraform.workspace)}"
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

# https://github.com/tillkuhn/yummy-aws/blob/master/terraform/terraform-apigw.tf
# great example for ddb tf
resource "aws_api_gateway_integration" "sub-prefix-integration" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.sub-prefix.id
  http_method = aws_api_gateway_method.sub-prefix-method.http_method
  integration_http_method = "POST"
  type = "AWS"
  uri = "arn:aws:apigateway:us-west-2:dynamodb:action/Query"
  credentials = "arn:aws:iam::504084586672:role/PoliGraphDataAccess"

  passthrough_behavior = "WHEN_NO_TEMPLATES"

  request_templates = {
    "application/json" = <<EOF
    {
      "TableName": "PoliVertexNameAutoComplete${terraform.workspace}",
      "KeyConditionExpression": "prefix = :key",
      "ExpressionAttributeValues": {
        ":key": {
          "S": "$input.params('prefix')"
        }
      }
    }
    EOF
  }
}

## Region Method: GET All regions - method response 200
resource "aws_api_gateway_method_response" "sub-prefix-response" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.sub-prefix.id
  http_method = aws_api_gateway_method.sub-prefix-method.http_method
  status_code = "200"
  response_parameters = {
    "method.response.header.Access-Control-Allow-Origin" = true
  }
}

## Mapping reference: https://docs.aws.amazon.com/de_de/apigateway/latest/developerguide/api-gateway-mapping-template-reference.html
## conditional? https://stackoverflow.com/questions/32511087/aws-api-gateway-how-do-i-make-querystring-parameters-optional-in-mapping-templa
resource "aws_api_gateway_integration_response" "sub-prefix-integration-response" {
  depends_on = [
    aws_api_gateway_integration.sub-prefix-integration
  ]
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.sub-prefix.id
  http_method = aws_api_gateway_method.sub-prefix-method.http_method
  status_code = aws_api_gateway_method_response.sub-prefix-response.status_code
  response_parameters = {
    "method.response.header.Access-Control-Allow-Origin" = "'*'"
  }
}

################################################################################
## DYNAMO-BATCH-GET POST REQUEST ######################################################
################################################################################
resource "aws_api_gateway_resource" "batch-get-item" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  parent_id = aws_api_gateway_resource.poli.id
  path_part = "batch-get-item"
}

resource "aws_api_gateway_method" "batch-get-item-method" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.batch-get-item.id
  http_method = "POST"
  authorization = "NONE"
}

# https://github.com/tillkuhn/yummy-aws/blob/master/terraform/terraform-apigw.tf
# great example for ddb tf
resource "aws_api_gateway_integration" "batch-get-item-integration" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.batch-get-item.id
  http_method = aws_api_gateway_method.batch-get-item-method.http_method
  integration_http_method = "POST"
  type = "AWS"
  uri = "arn:aws:apigateway:us-west-2:dynamodb:action/BatchGetItem"
  credentials = "arn:aws:iam::504084586672:role/PoliGraphDataAccess"
}

resource "aws_api_gateway_method_response" "batch-get-item-response" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.batch-get-item.id
  http_method = aws_api_gateway_method.batch-get-item-method.http_method
  status_code = "200"
  response_parameters = {
    "method.response.header.Access-Control-Allow-Origin" = true
  }
}

## Mapping reference: https://docs.aws.amazon.com/de_de/apigateway/latest/developerguide/api-gateway-mapping-template-reference.html
## conditional? https://stackoverflow.com/questions/32511087/aws-api-gateway-how-do-i-make-querystring-parameters-optional-in-mapping-templa
resource "aws_api_gateway_integration_response" "batch-get-item-integration-response" {
  depends_on = [
    aws_api_gateway_integration.batch-get-item-integration
  ]
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.batch-get-item.id
  http_method = aws_api_gateway_method.batch-get-item-method.http_method
  status_code = aws_api_gateway_method_response.batch-get-item-response.status_code
  response_parameters = {
    "method.response.header.Access-Control-Allow-Origin" = "'*'"
  }
}


################################################################################
## DYNAMO-GET POST REQUEST #####################################################
################################################################################
resource "aws_api_gateway_resource" "get-item" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  parent_id = aws_api_gateway_resource.poli.id
  path_part = "get-item"
}

resource "aws_api_gateway_method" "get-item-method" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.get-item.id
  http_method = "POST"
  authorization = "NONE"
}

# https://github.com/tillkuhn/yummy-aws/blob/master/terraform/terraform-apigw.tf
# great example for ddb tf
resource "aws_api_gateway_integration" "get-item-integration" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.get-item.id
  http_method = aws_api_gateway_method.get-item-method.http_method
  integration_http_method = "POST"
  type = "AWS"
  uri = "arn:aws:apigateway:us-west-2:dynamodb:action/GetItem"
  credentials = "arn:aws:iam::504084586672:role/PoliGraphDataAccess"
}

resource "aws_api_gateway_method_response" "get-item-response" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.get-item.id
  http_method = aws_api_gateway_method.get-item-method.http_method
  status_code = "200"
  response_parameters = {
    "method.response.header.Access-Control-Allow-Origin" = true
  }
}

## Mapping reference: https://docs.aws.amazon.com/de_de/apigateway/latest/developerguide/api-gateway-mapping-template-reference.html
## conditional? https://stackoverflow.com/questions/32511087/aws-api-gateway-how-do-i-make-querystring-parameters-optional-in-mapping-templa
resource "aws_api_gateway_integration_response" "get-item-integration-response" {
  depends_on = [
    aws_api_gateway_integration.get-item-integration
  ]
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.get-item.id
  http_method = aws_api_gateway_method.get-item-method.http_method
  status_code = aws_api_gateway_method_response.get-item-response.status_code
  response_parameters = {
    "method.response.header.Access-Control-Allow-Origin" = "'*'"
  }
}

################################################################################
## CORS (Mock Method) ##########################################################
################################################################################
resource "aws_api_gateway_method" "batch-get-item-cors-method" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.batch-get-item.id
  http_method = "OPTIONS"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "batch-get-item-cors-integration" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.batch-get-item.id
  http_method = aws_api_gateway_method.batch-get-item-cors-method.http_method
  type = "MOCK"
  depends_on = [
    aws_api_gateway_method.batch-get-item-cors-method]
  passthrough_behavior = "WHEN_NO_TEMPLATES"
  request_templates = {
    "application/json" = <<EOF
    {
      statusCode: 200
    }
    EOF
  }
}

resource "aws_api_gateway_method_response" "batch-get-item-cors-response" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.batch-get-item.id
  http_method = aws_api_gateway_method.batch-get-item-cors-method.http_method
  status_code = "200"
  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true,
    "method.response.header.Access-Control-Allow-Methods" = true,
    "method.response.header.Access-Control-Allow-Origin" = true
  }
  depends_on = [
    aws_api_gateway_method.batch-get-item-cors-method]
}

resource "aws_api_gateway_integration_response" "batch-get-item-cors-integration-response" {
  depends_on = [
    aws_api_gateway_integration.batch-get-item-cors-integration
  ]
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.batch-get-item.id
  http_method = aws_api_gateway_method.batch-get-item-cors-method.http_method
  status_code = aws_api_gateway_method_response.batch-get-item-cors-response.status_code
  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
    "method.response.header.Access-Control-Allow-Methods" = "'GET,OPTIONS,POST,PUT'",
    "method.response.header.Access-Control-Allow-Origin" = "'*'"
  }
}

################################################################################
## CORS (Mock Method) ##########################################################
################################################################################
resource "aws_api_gateway_method" "get-item-cors-method" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.get-item.id
  http_method = "OPTIONS"
  authorization = "NONE"
}

resource "aws_api_gateway_integration" "get-item-cors-integration" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.get-item.id
  http_method = aws_api_gateway_method.get-item-cors-method.http_method
  type = "MOCK"
  depends_on = [
    aws_api_gateway_method.get-item-cors-method]
  passthrough_behavior = "WHEN_NO_TEMPLATES"
  request_templates = {
    "application/json" = <<EOF
    {
      statusCode: 200
    }
    EOF
  }
}

resource "aws_api_gateway_method_response" "get-item-cors-response" {
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.get-item.id
  http_method = aws_api_gateway_method.get-item-cors-method.http_method
  status_code = "200"
  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = true,
    "method.response.header.Access-Control-Allow-Methods" = true,
    "method.response.header.Access-Control-Allow-Origin" = true
  }
  depends_on = [
    aws_api_gateway_method.get-item-cors-method]
}

resource "aws_api_gateway_integration_response" "get-item-cors-integration-response" {
  depends_on = [
    aws_api_gateway_integration.get-item-cors-integration
  ]
  rest_api_id = aws_api_gateway_rest_api.api.id
  resource_id = aws_api_gateway_resource.get-item.id
  http_method = aws_api_gateway_method.get-item-cors-method.http_method
  status_code = aws_api_gateway_method_response.get-item-cors-response.status_code
  response_parameters = {
    "method.response.header.Access-Control-Allow-Headers" = "'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token'",
    "method.response.header.Access-Control-Allow-Methods" = "'GET,OPTIONS,POST,PUT'",
    "method.response.header.Access-Control-Allow-Origin" = "'*'"
  }
}

################################################################################
## DEPLOYMENT ##################################################################
################################################################################
## Deploy the Gateway Stage
## it seems you have to update the variable to actually force a deployment
resource "aws_api_gateway_deployment" "deploy" {
  depends_on = [
    aws_api_gateway_resource.poli
  ]
  rest_api_id = aws_api_gateway_rest_api.api.id
  stage_name = lower(terraform.workspace)
  variables = {
    "answer" = var.deployment_id
  }
}
