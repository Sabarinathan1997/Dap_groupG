import numpy as np
import pandas as pd
from dagster import op, Out, In, get_dagster_logger
from dagster_pandas import PandasColumn, create_dagster_pandas_dataframe_type
from sqlalchemy import create_engine, exc
from sqlalchemy.pool import NullPool
from sqlalchemy.types import *
from pymongo import MongoClient, errors
postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/ev_db"
mongo_connection_string = "mongodb://dap:dap@127.0.0.1"
logger = get_dagster_logger()

#DataFrame for transforming
airpollutionDataFrame = create_dagster_pandas_dataframe_type(
    name="airpollutionDataFrame",
    columns=[
        PandasColumn.integer_column("pollution_level", non_nullable=False,
            ignore_missing_vals=True),
        PandasColumn.string_column("state", non_nullable=True),
        PandasColumn.datetime_column(
            "date",
            min_datetime=datetime(year=2012, month=1, day=1),
            max_datetime=datetime(year=2024, month=1, day=1)
        ),
        PandasColumn.integer_column("air_quality_index", non_nullable=False,
            ignore_missing_vals=True),
        PandasColumn.float_column("ozone_level", non_nullable=False,
            ignore_missing_vals=True),
        PandasColumn.float_column("quality_of_atmosphere", non_nullable=False,
            ignore_missing_vals=True)])

carSalesDataFrame = create_dagster_pandas_dataframe_type(
    name="carSalesDataFrame",
    columns=[
        PandasColumn.string_column("state", non_nullable=False,
            ignore_missing_vals=True),
        PandasColumn.string_column("fuel_type", non_nullable=False,
            ignore_missing_vals=True),
        PandasColumn.integer_column("age_of_car", non_nullable=False,
            ignore_missing_vals=True),
        PandasColumn.integer_column("emission", non_nullable=False,
            ignore_missing_vals=True)
        ])
evpopulationDataFrame = create_dagster_pandas_dataframe_type(
    name="evpopulationDataFrame",
    columns=[
        PandasColumn.string_column("State",non_nullable=False,
            ignore_missing_vals=True),
        PandasColumn.integer_column("Model Year",non_nullable=False,
            ignore_missing_vals=True),
        PandasColumn.integer_column("DOL Vehicle ID",non_nullable=False,
            ignore_missing_vals=True)])

# Air Pollution
air_data_df = None

@op(
    ins={"start": In(bool)},
    out=Out(airpollutionDataFrame)
)

def t_airpollution(start):

    mongo_connection_string = "mongodb://dap:dap@127.0.0.1"
    client = MongoClient(mongo_connection_string)
    db = client["dap"]
    global air_data_df 
    air_data_df = pd.json_normalize(list(db.dapairpollution.find({})))
    air_data_datatypes = dict(
        zip(air_data_df.columns, [object]*len(air_data_df.columns))
    )
    for column in ["temperature", "humidity","wind_speed","precipitation","pm25_level","pm10_level","ozone_level","carbon_monoxide_level"]:
        air_data_datatypes[column] = np.float64
    air_data_datatypes["pollution_level"] = np.int64
    air_data_datatypes["air_quality_index"] = np.int64
    air_data_datatypes["date"] = np.datetime64

    air_data_df = air_data_df.astype(air_data_datatypes)
    
    air_data_df["quality_of_atmosphere"] = air_data_df["pm25_level"] + air_data_df["pm10_level"] + air_data_df["carbon_monoxide_level"]
    air_data_df.drop(
        columns=["_id","pm25_level","pm10_level","carbon_monoxide_level","country","temperature","wind_speed","humidity", "wind_direction","precipitation"],
        axis=1,
        inplace=True
    )
    return air_data_df

# Fuel Cars
fuel_data_df = None

@op(
    ins={"start": In(bool)},
    out=Out(carSalesDataFrame)
)

def t_fuelcars(start):
    mongo_connection_string = "mongodb://dap:dap@127.0.0.1"
    client = MongoClient(mongo_connection_string)
    db = client["dap"]
    global fuel_data_df
    fuel_data_df = pd.json_normalize(list(db.dapfuelcars.find({})))
    fuel_data_datatypes = dict(
        zip(fuel_data_df.columns, [object]*len(fuel_data_df.columns))
    )
    fuel_data_datatypes["mileage"] = np.int64
    fuel_data_datatypes["price"] = np.int64
    fuel_data_datatypes["sale_date"] = np.datetime64
    
    fuel_data_df = fuel_data_df.astype(fuel_data_datatypes)
    
    fuel_data_df['sale_date'] = pd.to_datetime(fuel_data_df['sale_date'], format='%m/%d/%Y')
    current_date = datetime.now()
    fuel_data_df['age_of_car'] = (current_date - fuel_data_df['sale_date']).dt.days
    fuel_data_df['emission'] = (fuel_data_df['mileage'] / fuel_data_df['age_of_car']).astype(int)
    fuel_data_df.drop(
        columns=["_id","sale_date","mileage","car_make","country","price","dealer_name","dealer_email","dealer_phone","car_model"],
        axis=1,
        inplace=True
    )
    return fuel_data_df

#EV Population
ev_data_df = None

@op(
    ins={"start": In(bool)},
    out=Out(evpopulationDataFrame)
)

def t_evpopulation(start):
    
    mongo_connection_string = "mongodb://dap:dap@127.0.0.1"
    client = MongoClient(mongo_connection_string)
    db = client["dap"]
    global ev_data_df
    ev_data_df= pd.json_normalize(list(db.dapevpopulation.find({})))
    ev_data_datatypes = dict(
        zip(ev_data_df.columns, [object]*len(ev_data_df.columns)))
    ev_data_datatypes["Postal Code"] = np.int64
    ev_data_datatypes["Model Year"] = np.int64
    ev_data_datatypes["Electric Range"] = np.int64
    ev_data_datatypes["Base MSRP"] = np.int64
    ev_data_datatypes["Legislative District"] = np.int64
    ev_data_datatypes["DOL Vehicle ID"] = np.int64
    ev_data_datatypes["2020 Census Tract"] = np.int64
    
    ev_data_df = ev_data_df.astype(ev_data_datatypes)
    
    ev_data_df.drop(
        columns=["_id","VIN (1-10)","Model","Make","Electric Vehicle Type","Clean Alternative Fuel Vehicle (CAFV) Eligibility","Electric Range","Base MSRP","Legislative District", "City", "Postal Code", "Electric Utility", "Vehicle Location", "County","2020 Census Tract"],
        axis=1,
        inplace=True
    )
    return ev_data_df

#adding the ev data into postgreSQL
@op(
    ins={"ev_data_df": In(pd.DataFrame)},
    out=Out(bool)
)

def load(ev_data_df):
    try:
        postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/ev_db"
        # Create a connection to the PostgreSQL database
        engine = create_engine(
            postgres_connection_string,
            poolclass=NullPool
        )
        
        ev_datatypes = dict(
            zip(ev_data_df.columns,[VARCHAR]*len(ev_data_df.columns))
        )
        
        # Set columns with INT datatype
        ev_datatypes["Model Year"] = INT
        ev_datatypes["DOL Vehicle ID"] = BIGINT
        with engine.connect() as conn:

            rowcount = ev_data_df.to_sql(
                name="ev_data",
                schema="public",
                dtype=ev_datatypes,
                con=engine,
                index=False,
                if_exists="replace"
            )
            logger.info("{} records loaded".format(rowcount))

        engine.dispose(close=True)

        return rowcount > 0
    
    # Trap and handle any relevant errors
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False
   
#merging
merge_df = None

@op(
    ins={"air_data_df": In(pd.DataFrame), "fuel_data_df": In(carSalesDataFrame)},
    out=Out(pd.DataFrame)
)
def joins(air_data_df,fuel_data_df) -> pd.DataFrame:
    # Merge air_data and fuel_data
    ev_data_df = pd.DataFrame()
    load(ev_data_df) #calling ev_data_df
    global merge_df
    merge_df = air_data_df.merge(
        right=fuel_data_df,
        how="left",
        on="state"  
    )
    
    return merge_df
#Load
@op(
    ins={"merge_df": In(pd.DataFrame)},
    out=Out(bool)
)

def load(merge_df):
    try:
        postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/ev_db"
        # Create a connection to the PostgreSQL database
        engine = create_engine(
            postgres_connection_string,
            poolclass=NullPool
        )
        
        # Create a dictionary with column names as the key and the VARCHAR 
        # type as the value. This will be used to specify data types for the
        # created database. We will change some of these types later.
        database_datatypes = dict(
            zip(merge_df.columns,[VARCHAR]*len(merge_df.columns))
        )
        
        # Set date column to have the TIMESTAMP datatype
        database_datatypes["date"] = TIMESTAMP
        
        # Set columns with DOUBLE PRECISION datatype
        for column in ["ozone_level","quality_of_atmosphere"]:
            database_datatypes[column] = DECIMAL
        
        # Set columns with INT datatype
        for column in ["pollution_level","air_quality_index","age_of_car","emission"]:
            database_datatypes[column] = INT
            
        # Open the connection to the PostgreSQL server
        with engine.connect() as conn:
            
            # Store the data frame contents to the flight_weather 
            # table, using the dictionary of data types created
            # above and replacing any existing table
            rowcount = merge_df.to_sql(
                name="air_fuel",
                schema="public",
                dtype=database_datatypes,
                con=engine,
                index=False,
                if_exists="replace"
            )
            logger.info("{} records loaded".format(rowcount))
            
        # Close the connection to PostgreSQL and dispose of 
        # the connection engine
        engine.dispose(close=True)
        
        # Return the number of rows inserted
        return rowcount > 0
    
    # Trap and handle any relevant errors
    except exc.SQLAlchemyError as error:
        logger.error("Error: %s" % error)
        return False
