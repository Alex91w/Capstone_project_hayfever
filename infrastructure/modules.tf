module "fetch_weather" {
  source = "./fetch_weather"

  weather_table_name = aws_dynamodb_table.weather.name
  role_arn = local.role_arn
}