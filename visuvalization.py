from dagster import op, In, Out
import joypy
import sklearn
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text, exc
import pandas.io.sql as sqlio
import numpy as np
from sklearn.decomposition import PCA
import statsmodels
from statsmodels.tsa.seasonal import seasonal_decompose

@op(
    ins={"start": In(bool)},
    out=Out(pd.DataFrame),
)
def visualize(start):
    postgres_connection_string = "postgresql://dap:dap@127.0.0.1:5432/ev_db"

    query_string1 = """SELECT * FROM air_fuel;"""
    query_string2 = """SELECT * FROM ev_data"""

    try:
        engine = create_engine(postgres_connection_string)

        with engine.connect() as connection:
            air_fuel_df = sqlio.read_sql_query(text(query_string1), connection)
            ev_dataframe = sqlio.read_sql_query(text(query_string2), connection)

        # 1. Bar Plot - fuel_type & emission
        plt.figure(figsize=(15, 6))
        sns.barplot(x='fuel_type', y='emission', data=air_fuel_df)
        plt.title('Bar Plot: Emission by Fuel Type')
        plt.show()


        # 2. scatter Plot - Age of Cars by State
        plt.figure(figsize=(25, 6))
        sns.jointplot(x='pollution_level', y='age_of_car', data=air_fuel_df)  # Change to ev_dataframe
        plt.suptitle('Joint Plot: Pollution Level vs. Age of Cars')
        plt.show()


        # 3. Scree Plot - Emission vs. Age of Cars
        features_for_pca = air_fuel_df[['age_of_car', 'emission']]
        standardized_features = (features_for_pca - features_for_pca.mean()) / features_for_pca.std()
        pca = PCA()
        principal_components = pca.fit_transform(standardized_features)
        explained_variance_ratio = pca.explained_variance_ratio_
        # Plot the Scree Plot
        plt.plot(range(1, len(explained_variance_ratio) + 1), explained_variance_ratio, marker='o')
        plt.title('Scree Plot')
        plt.xlabel('Principal Component')
        plt.ylabel('Proportion of Variance Explained')
        plt.show()
        
        # 4. Histogram - Distribution of Pollution Levels by State
        plt.figure(figsize=(12, 6))
        sns.histplot(x='pollution_level', hue='state', data=air_fuel_df, bins=20, multiple='stack', palette='viridis')
        plt.title('Distribution of Pollution Levels by State')
        plt.xlabel('Pollution Level')
        plt.ylabel('Frequency')
        plt.show()

        # 5. Histogram - Distribution of Air Quality Index
        air_fuel_df['date'] = pd.to_datetime(air_fuel_df['date'])
        air_fuel_df.set_index('date', inplace=True)
        # Plotting a histogram for the 'air_quality_index' column
        plt.hist(air_fuel_df['air_quality_index'], bins=20, color='skyblue', edgecolor='black')
        plt.title('Histogram - Skewed Distribution of Air Quality Index')
        plt.xlabel('Air Quality Index')
        plt.ylabel('Frequency')
        plt.show()

        # 6. Pair Plot - Pairwise Relationships between Numerical Columns (Subplots)
        fig, axes = plt.subplots(nrows=2, ncols=2, figsize=(15, 10))
        # Pairwise relationship for 'age_of_car' and other columns
        sns.scatterplot(x='age_of_car', y='emission', data=air_fuel_df, ax=axes[0, 0])
        axes[0, 0].set_title('Age of Car vs. Emission')
        sns.scatterplot(x='age_of_car', y='pollution_level', data=air_fuel_df, ax=axes[0, 1])
        axes[0, 1].set_title('Age of Car vs. Pollution Level')
        # Pairwise relationship for 'emission' and other columns
        sns.scatterplot(x='emission', y='pollution_level', data=air_fuel_df, ax=axes[1, 0])
        axes[1, 0].set_title('Emission vs. Pollution Level')
        sns.scatterplot(x='emission', y='air_quality_index', data=air_fuel_df, ax=axes[1, 1])
        axes[1, 1].set_title('Emission vs. Air Quality Index')
        plt.suptitle('Pairwise Relationships between Numerical Columns')
        plt.tight_layout()
        plt.show()


        # 7. Heatmap - Correlation Matrix
        corr_matrix = air_fuel_df.corr()  # Change to air_fuel_df
        plt.figure(figsize=(15, 6))
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm')
        plt.title('Correlation Matrix')
        plt.show()

        # 8. Violin Plot - Distribution of Ozone Level by State
        plt.figure(figsize=(20, 6))
        sns.violinplot(x='state', y='ozone_level', data=air_fuel_df)
        plt.title('Distribution of Ozone Level by State')
        plt.show()

        # 9. Time Series Plot - Air Quality Index Over Time
        
        air_fuel_df['date'] = pd.to_datetime(air_fuel_df['date'])
        air_fuel_df.set_index('date', inplace=True)
        # Plotting a time series with correct x-axis range
        plt.figure(figsize=(25, 6))  # Adjust the figure size as needed
        sns.lineplot(x=air_fuel_df.index, y='air_quality_index', data=air_fuel_df)
        plt.title('Air Quality Index Over Time')
        plt.xlabel('Date')
        plt.ylabel('Air Quality Index')
        # Set x-axis limits based on the range of dates
        plt.xlim(air_fuel_df.index.min(), air_fuel_df.index.max())
        plt.show()

        # 10. countplot - Population of EV Vehicles
        plt.figure(figsize=(14, 8))
        sns.countplot(x='Model Year', data=ev_dataframe)
        plt.title('Population of EV Vehicles')
        plt.xticks(rotation=45, ha='right')  
        plt.show()
        
        # Close the database connection
        engine.dispose()

        return air_fuel_df  

    except exc.SQLAlchemyError as dbError:
        print("PostgreSQL Error:", dbError)
        raise 

    finally:
        # Close the database connection
        engine.dispose()
