from dagster import job
from extract import *
from transform_load import *
from visuvalization import *
 
@job
def etl():
    # Visualise data from PostgreSQL
    visualize(
        # Load the joined data into PostgreSQL
        load(
            # Join the flights and weather data
            joins(
                # Transform the stored evpopulation data
                t_evpopulation(
                    # Extract and store the evpopulation data
                    e_evpopulation()
                ),
                # Transform the stored fuelcars data
                t_fuelcars(
                    # Extract and store the fuelcars data
                    e_fuelcars()
                ),
                # Transform the stored airpollution data
                t_airpollution(
                    # Extract and store the airpollution data
                    e_airpollution()
                )
            )
        )
    )
