import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from pathlib import Path
import pandas as pd
import numpy as np
import os


def convert_csvs_to_parquet(dir_path=None):
    """
    Convert all the CSV files in a folder into a Parquet file
    :return:
    """
    if not dir_path:
        raise Exception("Please provide a path to the folder containing the CSV files")

    # Get all csv files in the folder
    files = os.listdir(dir_path)
    csv_files = [os.path.join(dir_path, f) for f in files if f.endswith(".csv")]

    # Read the CSV files into a Pandas DataFrame
    dfs = [pd.read_csv(f) for f in csv_files]
    files_dfs_dict = dict(zip(csv_files, dfs))

    # Create Parquet file for each file
    for f, df in files_dfs_dict.items():
        file_name = Path(f).stem + ".parquet"
        df.to_parquet(os.path.join(dir_path, file_name), engine="pyarrow")


def move_parquet_into_postgre(dir_path: str):
    """
    Move the parquet file into postgres
    :return:
    """
    if not dir_path:
        raise Exception("Please provide a path to the folder containing the CSV files")

        # Get all csv files in the folder
    files = os.listdir(dir_path)
    parquet_files = [os.path.join(dir_path, f) for f in files if f.endswith(".parquet")]    # Read the CSV files into a Pandas DataFrame
    dfs = [pd.read_parquet(f, engine="pyarrow") for f in parquet_files]
    files_dfs_dict = dict(zip(parquet_files, dfs))
    for f_path, df in files_dfs_dict.items():
        df.columns = [c.lower() for c in df.columns]  # postgres doesn't like capitals or spaces
        # Remove date and time columns
        if 'date' in df.columns and 'time' in df.columns:
            df = df.drop(['date', 'time'], axis=1)

        table_name = Path(f_path).stem
        engine = create_engine(f"postgresql://postgres:123123@localhost:5432/sensorsDB")

        df.to_sql(table_name+'_raw', engine)


def prepare_reading_data(path):
    # Load the data from parquet
    df = pd.read_parquet(os.path.join(path, "Readings.parquet"),
                         engine="pyarrow",
                         columns=['SensorID', 'Timestamp', 'ReadingValue'])
    # Convert to datetime
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='mixed', dayfirst=True)

    # Convert to UTC time
    df['Timestamp'] = df['Timestamp'].dt.tz_localize('UTC')

    return df


def query_2(df: pd.DataFrame = pd.DataFrame(), start_datetime: str = None, end_datetime: str = None):
    """
     Find the average, minimum, and maximum sensor readings for a
     specific sensor within a given time period.

    :return:
    """
    if df.empty:
        raise Exception("Please provide a DataFrame")
    if not start_datetime or not end_datetime:
        raise Exception("Please provide a start and end datetime")

    # Filter specific dates/period
    january_df = df[df['Timestamp'].between(start_datetime, end_datetime)]

    # Find the average, minimum, and maximum sensor readings .
    grouped_january_df = (january_df.groupby(['SensorID']).agg({'ReadingValue': ['mean', 'min', 'max']}).reset_index())

    # Save to csv
    grouped_january_df.to_csv("query_2_avg_min_max.csv", index=False)

    return grouped_january_df


def query_3(df: pd.DataFrame = pd.DataFrame(), threshold: float = None):
    """
    Find the sensors that experienced readings above a certain
    threshold value
    :return:
    """
    if df.empty:
        raise Exception("Please provide a DataFrame")
    if not threshold:
        raise Exception("Please provide a threshold value")

    # Find the sensors that experienced readings above a certain threshold value
    above_threshold_df = df[df['ReadingValue'] > threshold]

    # Save to csv
    above_threshold_df.to_csv("query_3_above_threshold.csv", index=False)

    return above_threshold_df


def query_4(df: pd.DataFrame = pd.DataFrame()):
    """
    Calculate the hourly, daily, and monthly averages for sensor readings and store the
    results in appropriate tables.
    Create a table for each of the above time periods.
    :return:
    """
    if df.empty:
        raise Exception("Please provide a DataFrame")

    hourly_df = pd.DataFrame()

    # Calculate the hourly, daily, and monthly averages for sensor readings
    df['hourly_avg'] = df.groupby(['SensorID', df['Timestamp'].dt.hour])['ReadingValue'].transform('mean')
    df['daily_avg'] = df.groupby(['SensorID', df['Timestamp'].dt.day])['ReadingValue'].transform('mean')
    df['monthly_avg'] = df.groupby(['SensorID', df['Timestamp'].dt.month])['ReadingValue'].transform('mean')

    # Store the results in appropriate tables
    hourly_df = df[['SensorID', 'Timestamp', 'hourly_avg']].drop_duplicates('hourly_avg')
    daily_df = df[['SensorID', 'Timestamp', 'daily_avg']].drop_duplicates('daily_avg')
    monthly_df = df[['SensorID', 'Timestamp', 'monthly_avg']].drop_duplicates('monthly_avg')

    # Save to csv
    hourly_df.to_csv("query_4_hourly.csv", index=False)
    daily_df.to_csv("query_4_daily.csv", index=False)
    monthly_df.to_csv("query_4_monthly.csv", index=False)

    return hourly_df, daily_df, monthly_df


def query_5(df: pd.DataFrame = pd.DataFrame()):
    """
    Identify trends in sensor readings over time, such as identifying
    sensors with increasing or decreasing readings.

    :param df:
    :return:
    """
    if df.empty:
        raise Exception("Please provide a DataFrame")

    # Get average dfs
    hourly_df = pd.read_csv(r'query_4_hourly.csv', parse_dates=['Timestamp'])
    daily_df = pd.read_csv(r'query_4_daily.csv', parse_dates=['Timestamp'])
    monthly_df = pd.read_csv(r'query_4_monthly.csv', parse_dates=['Timestamp'])

    dfs = {'hourly_avg': hourly_df,
           'daily_avg': daily_df,
           'monthly_avg': monthly_df}

    for avg_col, df in dfs.items():
        df['diff'] = df.groupby(['SensorID'])[avg_col].transform(lambda x: x.diff())
        df['diff'] = df['diff'].fillna(0)
        df['trend'] = np.where(df['diff'] > 0, '1', '-1')

    # Save to csv
    hourly_df.to_csv("query_5_hourly_trends.csv", index=False)
    daily_df.to_csv("query_5_daily_trends.csv", index=False)
    monthly_df.to_csv("query_5_monthly_trends.csv", index=False)

    # Plot the hourly_avg of each sensor on the same graph in different color

    for sensor_id in df['SensorID'].unique():
        sensor_df = hourly_df[hourly_df['SensorID'] == sensor_id]
        plt.plot(sensor_df['Timestamp'].dt.hour, sensor_df['hourly_avg'], label=sensor_id)

    plt.legend()
    plt.savefig('query_5_hourly_trends.jpg')
    plt.show()



    # We can see from the graph that sensor 1 is working from 0 to 10 and sensor 2 is working from 10 to 23.
    # and sensor 3 is working all the time.
    # also that the values of the sensors is loser in the night and higher in the day.

    # Plot the daily_avg of each sensor on the same graph in different color
    for sensor_id in df['SensorID'].unique():
        sensor_df = daily_df[daily_df['SensorID'] == sensor_id]
        plt.plot(sensor_df['Timestamp'].dt.day, sensor_df['daily_avg'], label=sensor_id)
    plt.legend()
    plt.savefig('query_5_daily_trends.jpg')
    plt.show()


    # We can see that sensor 2 has high values then the rest of the sensors in each day.

    # monthly_avg is short so it's not so interesting to plot it.

    return hourly_df, daily_df, monthly_df


def plot_trends():
    # Connect to postgres
    engine = create_engine(f"postgresql://postgres:123123@localhost:5432/sensorsDB")

    # Get table from postgres
    moving_averages_df = pd.read_sql_query('SELECT * FROM hourly_moving_averages', con=engine)

    # Filter sensor 3 (for example)
    moving_averages_df = moving_averages_df[moving_averages_df['sensorid'] == 3]

    # Plot both avg_readings and moving_average on the same graph
    plt.plot(moving_averages_df['hour'], moving_averages_df['avg_readings'], label='avg_reading', color='red')
    plt.plot(moving_averages_df['hour'], moving_averages_df['moving_average'], label='moving_average', color='blue')
    plt.legend()
    plt.savefig('hourly_trend_of_sensor_3_query_5.jpg')
    plt.show()


def main():
    data_dir = r"C:\Idan\DataYoga\Data"
    convert_csvs_to_parquet(dir_path=data_dir)
    move_parquet_into_postgre(dir_path=data_dir)
    reading_df = prepare_reading_data(path=data_dir)
    grouped_january_df = query_2(df=reading_df, start_datetime='2020-01-01 00:00:00', end_datetime='2020-01-31 00:00:00')
    above_threshold_df = query_3(df=reading_df, threshold=120)
    hourly_df, daily_df, monthly_df = query_4(df=reading_df)
    hourly_df_with_trends, daily_df_with_trends, monthly_df_with_trends = query_5(df=reading_df)
    plot_trends()

if __name__ == '__main__':
    main()

