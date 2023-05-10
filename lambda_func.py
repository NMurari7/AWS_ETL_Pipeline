import boto3
import pandas as pd
import json
from io import StringIO

s3 = boto3.client('s3')


def lambda_handler(event, context):
    # s3://uber-murari/raw-uber/uber_data.csv
    # Replace 'bucket_name' and 'file_name' with the appropriate values for your S3 object
    df_list = []
    obj = s3.get_object(Bucket='uber-murari', Key='raw-uber/uber_data.csv')
    
    # Load the CSV data into a Pandas DataFrame
    df = pd.read_csv(obj['Body'])
    # print(df.head())
    
    # PREPROCESS
    
    # delete rows with pickup, drop lat and long with 0.0
    df = df[(df["dropoff_latitude"] != 0.0) & (df["pickup_latitude"] != 0.0) & (df["dropoff_longitude"] != 0.0) & (df["pickup_longitude"] != 0.0)]
    
    # converting datatype from string to datetime
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df = df.drop_duplicates().reset_index(drop=True)
    df['trip_id'] = df.index
 
    print(df.head())
    
    
    # datetime_dim dimesion table
    datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
    datetime_dim['tpep_pickup_datetime'] = datetime_dim['tpep_pickup_datetime']
    datetime_dim['pick_hour'] = datetime_dim['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = datetime_dim['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month'] = datetime_dim['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year'] = datetime_dim['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_weekday'] = datetime_dim['tpep_pickup_datetime'].dt.weekday
    
    datetime_dim['tpep_dropoff_datetime'] = datetime_dim['tpep_dropoff_datetime']
    datetime_dim['drop_hour'] = datetime_dim['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_dim['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_dim['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_dim['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_weekday'] = datetime_dim['tpep_dropoff_datetime'].dt.weekday
    
    
    datetime_dim['datetime_id'] = datetime_dim.index
    
    # datetime_dim = datetime_dim.rename(columns={'tpep_pickup_datetime': 'datetime_id'}).reset_index(drop=True)
    datetime_dim = datetime_dim[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday',
                                 'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]
    print("datetime_dim")
    df_list.append( (datetime_dim, "datetime_dim") )
    print(datetime_dim.head())
    
    # passenger_count_dim dimension table
    passenger_count_dim = df[['passenger_count']].reset_index(drop=True)
    passenger_count_dim['passenger_count_id'] = passenger_count_dim.index
    passenger_count_dim = passenger_count_dim[['passenger_count_id','passenger_count']]
    print("passenger_count_dim")
    print(passenger_count_dim.head())
    df_list.append( (passenger_count_dim, "passenger_count_dim") )
    
    
    # trip_distance_dim dimesion table
    trip_distance_dim = df[['trip_distance']].reset_index(drop=True)
    trip_distance_dim['trip_distance_id'] = trip_distance_dim.index
    trip_distance_dim = trip_distance_dim[['trip_distance_id','trip_distance']]
    print("trip_distance_dim")
    print(trip_distance_dim.head())
    df_list.append( (trip_distance_dim, "trip_distance_dim") )
    
    # rate_code_dim dimension table
    rate_code_type = {
    1:"Standard rate",
    2:"JFK",
    3:"Newark",
    4:"Nassau or Westchester",
    5:"Negotiated fare",
    6:"Group ride"
    }
    
    rate_code_dim = df[['RatecodeID']].reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim.index
    rate_code_dim['rate_code_name'] = rate_code_dim['RatecodeID'].map(rate_code_type)
    rate_code_dim = rate_code_dim[['rate_code_id','RatecodeID','rate_code_name']]
    print("rate_code_dim")
    print(rate_code_dim.head())
    df_list.append( (rate_code_dim, "rate_code_dim") )
    
    # pickup_location_dim dimension table
    pickup_location_dim = df[['pickup_longitude', 'pickup_latitude']].reset_index(drop=True)
    pickup_location_dim['pickup_location_id'] = pickup_location_dim.index
    pickup_location_dim = pickup_location_dim[['pickup_location_id','pickup_latitude','pickup_longitude']] 
    print("pickup_location_dim")
    print(pickup_location_dim.head())
    df_list.append( (pickup_location_dim, "pickup_location_dim") )
    
    
    # payment_type_dim dimesion table
    payment_type_name = {
    1:"Credit card",
    2:"Cash",
    3:"No charge",
    4:"Dispute",
    5:"Unknown",
    6:"Voided trip"
    }
    payment_type_dim = df[['payment_type']].reset_index(drop=True)
    payment_type_dim['payment_type_id'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_id','payment_type','payment_type_name']]
    print("payment_type_dim")
    print(payment_type_dim.head())
    df_list.append( (payment_type_dim, "payment_type_dim") )
    
    # dropoff_location_dim dimesion table
    dropoff_location_dim = df[['dropoff_longitude', 'dropoff_latitude']].reset_index(drop=True)
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_dim.index
    dropoff_location_dim = dropoff_location_dim[['dropoff_location_id','dropoff_latitude','dropoff_longitude']]
    print("dropoff_location_dim")
    print(dropoff_location_dim.head())
    df_list.append( (dropoff_location_dim, "dropoff_location_dim") )
    
    
    
    # fact_table
    fact_table = df.merge(passenger_count_dim, left_on='trip_id', right_on='passenger_count_id') \
             .merge(trip_distance_dim, left_on='trip_id', right_on='trip_distance_id') \
             .merge(rate_code_dim, left_on='trip_id', right_on='rate_code_id') \
             .merge(pickup_location_dim, left_on='trip_id', right_on='pickup_location_id') \
             .merge(dropoff_location_dim, left_on='trip_id', right_on='dropoff_location_id')\
             .merge(datetime_dim, left_on='trip_id', right_on='datetime_id') \
             .merge(payment_type_dim, left_on='trip_id', right_on='payment_type_id') \
             [['trip_id','VendorID', 'datetime_id', 'passenger_count_id',
               'trip_distance_id', 'rate_code_id', 'store_and_fwd_flag', 'pickup_location_id', 'dropoff_location_id',
               'payment_type_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
               'improvement_surcharge', 'total_amount']]
    print('Fact table')
    print(fact_table.head())
    df_list.append( (fact_table, "fact_table") )
    
    
    
    # Write all dimension tables and fact table to s3 location
    
    for i in df_list:
        df_name = i[0]
        filename = i[1]
        csv_buffer = StringIO()
        df_name.to_csv(csv_buffer, index=False)
        s3.put_object(Body=csv_buffer.getvalue(), Bucket='uber-murari', Key=f'star-schema/{filename}/{filename}.csv')
    
            

    return {
        'statusCode': 200,
        'body': json.dumps('succesfully uploaded files to s3')
    }
