# build fetch function
cd fetch_weather_lambda
zip -r ../../infrastructure/build/fetch_weather_lambda.zip .
cd .. 

# build requests layer
cd requests_layer
pip install -r requirements.txt -t python/lib/python3.9/site-packages
zip -r ../../infrastructure/build/requests_layer.zip .
cd ..