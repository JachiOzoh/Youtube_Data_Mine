from dotenv import load_dotenv #pip install python-dotenv
from etl_utils.extract import extract_data
from etl_utils.transform import transform_data
from etl_utils.validate import validate_data
from etl_utils.load import load_data

def main():
    """
    Main function to run the ETL process.
    It extracts data from YouTube API, transforms it, and loads it into a SQL Server database.
    """
    load_dotenv() # Load environment variables from .env file
    video_df, channel_stats_df = extract_data()
    video_df, channel_stats_df = transform_data(video_df, channel_stats_df)
    validate_data(video_df, channel_stats_df)
    load_data(video_df, channel_stats_df)
    print("ETL completed successfully.")
    # video_df.head().to_csv('video_data_exc.csv', index=False)
    # channel_stats_df.head().to_csv('channel_data_exc.csv', index=False)

if __name__ == "__main__":
    main()

