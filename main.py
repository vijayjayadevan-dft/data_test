import requests
import json
import logging
import pandas as pd


def get_data_from_api(url, filename):
    '''
    Function to get data from open-meteo weather API

    Args:
        url (str): API endpoint url
        filename (str): name of the file to store the API data temporarily

    Returns:
        None
    '''
    # Make the API request
    try:
        response = requests.get(url)
        # Check the response status code
        if response.status_code == 200:
            logging.info("API request successful")
            # saving the response data locally
            with open(filename, "w") as f:
                json.dump(response.json(), f)
        else:
            logging.error("API request failed")
            raise Exception("API request failed: {}".format(response.status_code))
    except Exception as e:
        logging.error("An error occurred: {}".format(e))

def do_data_transformations(filename, output_filename):
    '''
        Function to do data transformations
    Args:
        filename (str): name of the file to store the API data temporarily
        output_filename (str): name of the output parquet file

    Returns:
        None
    '''
    # Read the JSON file to memory
    with open(filename) as f:
        d = json.load(f)
    
    # Normalise json file and load as df
    try:
        df = pd.json_normalize(d)
        logging.debug(df.keys())

        # explode list columns to rows
        df = df.explode(['hourly.time', 'hourly.temperature_2m', 'hourly.rain', 'hourly.showers', 'hourly.visibility']).reset_index(drop=True)
        logging.debug(df)
    
        # save the vale header
        latitude = df['latitude'].values[0]
        longitude = df['longitude'].values[0]
        elevation = df['elevation'].values[0]   
        generationtime_ms = df['generationtime_ms'].values[0]   
        utc_offset_seconds = df['utc_offset_seconds'].values[0]
        timezone = df['timezone'].values[0]
        temperature_unit = df['hourly_units.temperature_2m'].values[0]
        rain_unit = df['hourly_units.rain'].values[0]
        showers_unit = df['hourly_units.showers'].values[0]
        visibility_unit = df['hourly_units.visibility'].values[0]

        # remove unwanted columns and tidy up
        df = df[['hourly.time', 'hourly.temperature_2m', 'hourly.rain', 'hourly.showers', 'hourly.visibility']]
        df = df.rename(columns={'hourly.time': 'time', 'hourly.temperature_2m': 'temperature_2m', 'hourly.rain':'rain', 'hourly.showers':'showers', 'hourly.visibility': 'visibility'})
        logging.debug(df)

        # Group the data by day
        df['time'] = pd.to_datetime(df['time'])
        df = df.groupby(df['time'].dt.date).agg({'temperature_2m': ['mean', 'min', 'max'], 'rain': ['sum', 'min', 'max'], 'showers': ['sum', 'min', 'max'], 'visibility': ['mean', 'min', 'max']})
        logging.debug(df)

        # output file to parquet
        df.to_parquet(output_filename)
    except Exception as e:
        logging.error("An error occurred: {}".format(e))



if __name__ == "__main__":
    # variables
    url = "https://api.open-meteo.com/v1/forecast?latitude=51.5085&longitude=-0.1257&hourly=temperature_2m,rain,showers,visibility&past_days=31"
    filename = "sample_data.json"
    output_filename = "output_data.parquet"
    # Get the data from the API
    get_data_from_api(url, filename)
    do_data_transformations(filename)