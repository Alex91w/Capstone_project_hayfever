resource "aws_lambda_function" "fetch_weather_lambda" {

  filename      = "build/fetch_weather_lambda.zip"
  function_name = "fetch_weather_lambda"
  role          = var.role_arn
  handler       = "fetch_weather.handle"

  source_code_hash = filebase64sha256("build/fetch_weather_lambda.zip")

  runtime = "python3.9"
  timeout = 600
  layers = [ aws_lambda_layer_version.requests_layer.arn ]

  environment {
    variables = {
      WEATHER_TABLE_NAME = var.weather_table_name
    }
  }
}
