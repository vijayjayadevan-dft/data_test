Python program to get weather data from OpenMeteo API.

<h2>Task:</h2>
We have a requirement to ingest weather data from an open API. This data is in json format,
published hourly and does not need a key to access it.
Write the python code required to pull the API data, aggregate the various fields to a daily total and
output as a parquet file. This code may be structured however you think best, using any packages or
libraries you deem appropriate but, your code must have sufficient error handling as well as unit tests
to ensure the code is robust and accurate.
The API URL is:
https://api.open-meteo.com/v1/forecast?latitude=51.5085&longitude=-0.1257&hourly=temperature_2
m,rain,showers,visibility&past_days=31

<h2>Steps:</h2>
Step1 - get data from API to storage
Step2 - read, process json data, do transformations and write output as parquet file.


