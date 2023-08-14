import pandas as pd
import datetime
import boto3
import json
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Read environment variables
        source_bucket_name = os.environ['SOURCE_BUCKET_NAME']
        destination_bucket_name = os.environ['DESTINATION_BUCKET_NAME']
        stock_news_file = 'stocks_news.csv'
        stock_hist_file = 'stocks_hist.csv'
        
        if not check_csv_file_exist(source_bucket_name, stock_news_file) or not check_csv_file_exist(source_bucket_name, stock_hist_file):
            return {
                'statusCode': 400,
                'body': json.dumps('CSV files not found in the source bucket.')
            }
        
        # Step 1: Read the CSV file from the source bucket
        response = s3.get_object(Bucket=source_bucket_name, Key=stock_news_file)
        df = pd.read_csv(response['Body'])
        print(df.head())
        
        # Checking the column names and NaN values in them
        # Display the column names
        print("Column Names:".format(df.columns))
        
        # Step 2: Display the first few rows of the DataFrame to inspect the data
        # Check the number of NaN values in each column
        na_counts = df.isna().sum()
        # Display the number of NaN values in each column
        print("\nNumber of NaN values in each column:")
        print(na_counts)
        
        # Step 3: Dropping Unnecessary Columns
        df.drop(columns=['image', 'category'], inplace=True)
        print(df.head(10))
        # Converting `datetime` from UNIX format to normal
        df['datetime_norm'] = df['datetime'].apply(lambda timestamp: datetime.datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S'))
        print(df.head())
        print(len(df))
        
        # Step 4: Checking the min and max dates in each stock which is under 'related'
        ticker_dates = df.groupby('related')['datetime_norm'].agg(['min', 'max'])
        # Print the results
        print("First Date and Last Date for Each Ticker:")
        print(ticker_dates)
        
        print("It's not going beyond this")
        
        # Step 5: Define the date range
        # start_date = pd.to_datetime('2023-07-20')
        # end_date = pd.to_datetime('2023-07-28')
        # # Use boolean indexing to filter the rows within the date range
        # df = df[(df['datetime_norm'] >= start_date) & (df['datetime_norm'] <= end_date)]
        # # Get the number of rows within the date range
        # num_rows_within_date_range = len(df)
        # print("Number of rows between", start_date.date(), "and", end_date.date(), ":", num_rows_within_date_range)

        df['datetime_norm'] = pd.to_datetime(df['datetime_norm'])
        df = df[df['datetime_norm'] >= '2023-07-20']
        df.reset_index(drop=True, inplace=True)
        print("Rows before 2023-01-01 have been dropped, and the filtered DataFrame is saved to 'filtered_stocks_news.csv'.")

        df['datetime_norm'] = pd.to_datetime(df['datetime_norm'])

        # Step 6: Group by 'related' and 'date', then find the latest headline for each group
        latest_headlines = df.groupby(['related', df['datetime_norm'].dt.date], as_index=False).last()
        print("Latest headlines for each stock group on each date saved to 'latest_headlines.csv'.")
        print(latest_headlines.head())
        
        # Convert 'datetime_norm' to datetime format
        df['datetime_norm'] = pd.to_datetime(df['datetime_norm'])
        df = df[df['datetime_norm'].dt.dayofweek < 5]
        df.reset_index(drop=True, inplace=True)
        print("Rows corresponding to weekends have been dropped, and the filtered DataFrame is saved to 'stocks_news_merge.csv'.")

        df_sentiment = latest_headlines

        # Step 7: Merging Process
        response = s3.get_object(Bucket=source_bucket_name, Key=stock_hist_file)
        df_hist = pd.read_csv(response['Body'])
        print(df_hist.head())
        
        # Convert 'datetime_norm' and 'Date' columns to datetime format
        df_sentiment['datetime_norm'] = pd.to_datetime(df_sentiment['datetime_norm'])
        df_hist['Date'] = pd.to_datetime(df_hist['Date'])
        
        # Extract only the date (day, month, year) from the 'datetime_norm' column
        df_sentiment['datetime_norm'] = df_sentiment['datetime_norm'].dt.date
        df_hist['Date'] = df_hist['Date'].dt.date
        
        # Merge the DataFrames based on 'datetime_norm', 'ticker', and 'Date'
        merged_df = df_sentiment.merge(df_hist, left_on=['datetime_norm', 'related'], right_on=['Date', 'ticker'], how='inner')
        
        # Drop the duplicate 'Date' and 'ticker' columns
        merged_df.drop(columns=['Date', 'ticker'], inplace=True)
        print("Data has been merged based on dates and stock groups, and the merged DataFrame is saved to 'finbert_stocks_input.csv'.")
        print(merged_df.head())
        print("Merged DataFrame shape:", merged_df.shape)
        
        # Check for any duplicate rows
        duplicate_rows = merged_df.duplicated()
        if duplicate_rows.any():
            print("Warning: Duplicate rows found in the merged DataFrame!")
        
        # For example, you can check the data types and unique values in the 'related' column
        print("Data types in the 'related' column:")
        print(merged_df['related'].dtype)
        print("Unique values in the 'related' column:")
        print(merged_df['related'].unique())
        

        # Step 8: Save the DataFrame to the destination bucket
        output_file_name = 'processed_stocks_news.csv'
        csv_buffer = merged_df.to_csv(index=False).encode()
        s3.put_object(Bucket=destination_bucket_name, Key=output_file_name, Body=csv_buffer)

        # Return a success response
        return {
            'statusCode': 200,
            'body': json.dumps('Data processing completed and saved to destination bucket!')
        }

    except Exception as e:
        # Return an error response in case of any exceptions
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

def check_csv_file_exist(bucket_name, file_name):
    try:
        s3.head_object(Bucket=bucket_name, Key=file_name)
        return True
    except Exception as e:
        # If the file does not exist, an exception will be raised
        return False