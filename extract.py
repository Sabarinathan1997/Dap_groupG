import xmltodict
import pandas as pd
from dagster import op, Out, In, get_dagster_logger
from datetime import date, datetime
from pymongo import MongoClient, errors
mongo_connection_string = "mongodb://dap:dap@127.0.0.1"
logger = get_dagster_logger()
@op(
    out=Out(bool)
)
def e_airpollution() -> bool:
    result = True
    try:
        client = MongoClient(mongo_connection_string)
        db = client["dap"]
        collection = db["dapairpollution"]

        with open('air_pollution_WA.xml') as air_file:
            air_data = air_file.read()
            dict1_data = xmltodict.parse(air_data)

        for pollution_record in dict1_data.get('pollution', {}).get('records', []):
            collection.insert_one({
                'pollution_level': pollution_record.get('pollution_level', ''),
                'country': pollution_record.get('country', ''),
                'state': pollution_record.get('state', ''),
                'temperature': pollution_record.get('temperature', ''),
                'date': pollution_record.get('date', ''),
                'humidity': pollution_record.get('humidity', ''),
                'wind_speed': pollution_record.get('wind_speed', ''),
                'wind_direction': pollution_record.get('wind_direction', ''),
                'precipitation': pollution_record.get('precipitation', ''),
                'air_quality_index': pollution_record.get('air_quality_index', ''),
                'ozone_level': pollution_record.get('ozone_level', ''),
                'pm25_level': pollution_record.get('pm25_level', ''),
                'pm10_level': pollution_record.get('pm10_level', ''),
                'carbon_monoxide_level': pollution_record.get('carbon_monoxide_level', '')
                })
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False
    return result
@op(
    out=Out(bool)
)

def e_fuelcars() -> bool:
    result = True
    try:
        client = MongoClient(mongo_connection_string)
        db = client["dap"]
        collection = db["dapfuelcars"]
        
        with open('fuel_car_sales_WA.xml') as fuel_file:
            fuel_data = fuel_file.read()
            dict2_data = xmltodict.parse(fuel_data)

        for sales_cars in dict2_data.get('sales', {}).get('cars', []):
            collection.insert_one({
                'car_make': sales_cars.get('car_make', ''),
                'car_model': sales_cars.get('car_model', ''),
                'country': sales_cars.get('country', ''),
                'state': sales_cars.get('state', ''),
                'fuel_type': sales_cars.get('fuel_type', ''),
                'mileage': int(sales_cars.get('mileage', 0)),
                'price': float(sales_cars.get('price', 0)),
                'dealer_name': sales_cars.get('dealer_name', ''),
                'dealer_email': sales_cars.get('dealer_email', ''),
                'dealer_phone': sales_cars.get('dealer_phone', ''),
                'sale_date': sales_cars.get('sale_date', '')
                })
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False
    return result
@op(
    out=Out(bool)
)

def e_evpopulation() -> bool:
    result = True
    try:
        mongo_connection_string = "mongodb://dap:dap@127.0.0.1"
        client = MongoClient(mongo_connection_string)
        db = client["dap"]
        collection = db["dapevpopulation"]
        df = pd.read_csv('Electric_Vehicle_Population_WA Data.csv')
        records = df.to_dict(orient='records')
        collection.insert_many(records)
    except Exception as err:
        logger.error("Error: %s" % err)
        result = False
    return result
    
