import googleapiclient.discovery
import pandas as pd
import urllib
import isodate
import os
import sqlalchemy as sa

from great_expectations.dataset import PandasDataset
from dotenv import load_dotenv #pip install python-dotenv


def get_chan_stats(youtube,channel_ids):
    """
    Return channel statistics: title, subscriber count, view count, video count, upload playlist
    Params:
    
    youtube: the build object from googleapiclient.discovery
    channel_ids: list of channel IDs
    
    Returns:
    dataframe containing the channel statistics for all channels in the provided list: title, subscriber count, view count, video count, upload playlist
    
    """
    try:
        request = youtube.channels().list(
                part='snippet,contentDetails,statistics',
                id=','.join(channel_ids))
        response = request.execute()
    except Exception as e:
        print(f"Error: An unexpected error occurred while fetching channel data: {e}")
    data=[]
    for i in range(len(response['items'])):
        # Access channel data within the nested JSON structure
        data_dict = dict(channelName = response['items'][i]['snippet']['title'],
                    subscribers = response['items'][i]['statistics']['subscriberCount'],
                    views = response['items'][i]['statistics']['viewCount'],
                    totalVideos = response['items'][i]['statistics']['videoCount'],
                    playlistId = response['items'][i]['contentDetails']['relatedPlaylists']['uploads'])
        data.append(data_dict)
    
        
    return pd.DataFrame(data)

def get_video_ids(youtube, playlist_id):
    """
    Get list of video IDs of all videos in the given playlist
    Params:
    
    youtube: the build object from googleapiclient.discovery
    playlist_id: playlist ID of the channel
    
    Returns:
    list of video IDs of all videos in the playlist
    
    """
    try:
        request = youtube.playlistItems().list(
                    part='contentDetails',
                    playlistId = playlist_id,
                    maxResults = 50)
        response = request.execute()
    except Exception as e:
        print(f"Error: An unexpected error occurred while fetching video id\'s: {e}")    
    video_ids = []
    
    for i in range(len(response['items'])):
        video_ids.append(response['items'][i]['contentDetails']['videoId'])
        
    next_page_token = response.get('nextPageToken')
    more_pages = True
    
    while more_pages:
        if next_page_token is None:
            more_pages = False
        else:
            request = youtube.playlistItems().list(
                        part='contentDetails',
                        playlistId = playlist_id,
                        maxResults = 50,
                        pageToken = next_page_token)
            response = request.execute()
    
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['contentDetails']['videoId'])
            
            next_page_token = response.get('nextPageToken')
        
    return video_ids

def get_video_details(youtube, video_ids):
    """
    Get video statistics of all videos with given IDs
    Params:
    
    youtube: the build object from googleapiclient.discovery
    video_ids: list of video IDs
    
    Returns:
    dataframe with the following video statistics:
        'channelTitle', 'title', 'description', 'tags', 'publishedAt'
        'viewCount', 'likeCount', 'favoriteCount', 'commentCount'
        'duration', 'definition', 'caption'
    """
        
    all_video_info = []
    try:
        for i in range(0, len(video_ids), 50):
            request = youtube.videos().list(
                part='snippet,contentDetails,statistics',
                id=','.join(video_ids[i:i+50])
            )
            response = request.execute() 
    
            for video in response['items']:
                stats_to_keep = {'snippet': ['channelTitle', 'title', 'description', 'tags', 'publishedAt'],
                             'statistics': ['viewCount', 'likeCount', 'favouriteCount', 'commentCount'],
                             'contentDetails': ['duration', 'definition', 'caption']
                            }
                video_info = {}
                video_info['video_id'] = video['id']
    
                for k in stats_to_keep.keys():
                    for v in stats_to_keep[k]:
                        try:
                            video_info[v] = video[k][v]
                        except:
                            video_info[v] = None
    
                all_video_info.append(video_info)        
    except Exception as e:
            print(f"Error: An unexpected error occurred while fetching video information: {e}")
    return pd.DataFrame(all_video_info)

def process_video_data(df):
    df = df.drop(['tags', 'favouriteCount'], axis=1)
    df.dropna()
    # Convert relevant columns to numeric
    numeric_cols = ['viewCount', 'likeCount', 'commentCount']
    df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors= 'coerce', axis=1)
    df[numeric_cols] = df[numeric_cols].astype('Int64')
    # Create a publish DOW (Day Of Week) column
    df['publishedAt'] =  pd.to_datetime(df['publishedAt']) 
    df['publishDayName'] = df['publishedAt'].apply(lambda x: x.strftime("%A"))
    df['publishedDate'] = df['publishedAt'].dt.date
    df['publishedTime'] = df['publishedAt'].dt.time

    return df

def total_seconds(iso_series):
    """
    Convert ISO format for duration into duration as total seconds
    Params:
    
    iso_series: the pandas series with duration listed in ISO format
    
    Returns:
    Pandas series with duration as total seconds
    
    """
    # Parse ISO8601 to timedelta
    td = isodate.parse_duration(iso_series)                   
    total_secs = int(td.total_seconds())
    
    return total_secs
    
def to_hhmmss(iso_series):
    """
    Convert ISO format for duration into standard colon seperated format
    Params:
    
    iso_series: the pandas series with the durations in ISO format
    
    Returns:
    Pandas series with duration in standard colon seperated format
    
    """
    # Parse ISO8601 to timedelta
    td = isodate.parse_duration(iso_series)                   
    total_secs = int(td.total_seconds())
    
    h = total_secs // 3600
    m = (total_secs % 3600) // 60
    s = total_secs % 60
    
    return f"{h:02}:{m:02}:{s:02}"

def extract_data():
    """
    Extract data from YouTube API for a list of channels
    Params:
    None
    
    Returns:
    video_df: DataFrame with video data
    channel_stats_df: DataFrame with channel statistics
    
    """
    # Load environment variables from .env file
    load_dotenv()
    api_key= os.getenv('API_KEY')
    api_service_name = 'youtube'
    api_version = 'v3'
    channel_ids=[  'UCtYLUTtgS3k1Fg4y5tAhLbw', # Statquest
                   'UCCezIgC97PvUuR4_gbFUs5g', # Corey Schafer
                   'UCfzlCWGWYyIQ0aLC5w48gBQ', # Sentdex
                   'UCNU_lfiiWBdtULKOw6X0Dig', # Krish Naik
                   'UCzL_0nIe8B4-7ShhVPfJkgw', # DatascienceDoJo
                   'UCLLw7jmFsvfIVaUFsLs8mlQ', # Luke Barousse 
                   'UCiT9RITQ9PW6BhXK0y2jaeg', # Ken Jee
                   'UC7cs8q-gJRlGwj4A8OmCmXg', # Alex the analyst
                   'UC2UXDak6o7rBm23k3Vv5dww', # Tina Huang
                   'UCDybamfye5An6p-j1t2YMsg', # Mo Chen
                   'UCAq9f7jFEA7Mtl3qOZy2h1A', # Data With Zach
                   'UC8_RSKwbU1OmZWNEoLV1tQg', # Data With Baraa
                   'UCmLGJ3VYBcfRaWbP6JLJcpA', # Seattle Data Guy
                   'UCFp1vaKzpfvoGai0vE5VJ0w', # Guy in a Cube
                   'UCh9nVJoWXmFb7sLApWGcLPQ', # Codebasics
                ]
    
    youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=api_key)
    channel_stats_df = get_chan_stats(youtube, channel_ids)
    video_df = pd.DataFrame()
    
    for chan in channel_stats_df['channelName'].unique():
        print("Getting video information for: " + chan)
        playlist_id = channel_stats_df.loc[channel_stats_df['channelName']== chan, 'playlistId'].iloc[0]
        video_ids = get_video_ids(youtube, playlist_id)
        
        # get video data
        video_data = get_video_details(youtube, video_ids)
        # append video data  
        video_df = video_df._append(video_data, ignore_index=True)
    return video_df, channel_stats_df

def transform_data(video_df, channel_stats_df):
    """
    Drops unnecessary columns, converts ISO duration to total seconds and standard colon separated format
    and renames columns to be more readable.
    Params:
    video_df: DataFrame with video data
    channel_stats_df: DataFrame with channel statistics
    
    Returns:
    video_df: Transformed DataFrame with video data
    channel_stats_df: Transformed DataFrame with channel statistics
    
    """
    video_df= process_video_data(video_df)
    # Filter out videos longer than 24 hours. Seconds in a day = 86400
    video_df=video_df[~(video_df['duration'].astype(str).apply(total_seconds) > 86400)]
    video_df['durationSecs'] = video_df['duration'].astype(str).apply(total_seconds)
    video_df['durationStandard'] = video_df['duration'].apply(to_hhmmss)
    
    video_df.rename(columns={'channelTitle': 'channel_title',
                       'publishedAt': 'published_at',
                       'likeCount': 'like_count',
                       'favouriteCount': 'favourite_count',
                       'commentCount': 'comment_count',
                       'durationSecs': 'duration_secs',
                       'durationStandard': 'duration_standard',
                       'viewCount': 'view_count',
                       'publishDayName': 'publish_day_name' ,
                       'publishedDate': 'publish_date' ,
                       'publishedTime': 'publish_time'
                        }, inplace=True)
    
    channel_stats_df.rename(columns={'channelName': 'channel_name',
                       'totalVideos': 'total_videos',
                       'playlistId': 'playlist_id'
                        }, inplace=True)
    return video_df, channel_stats_df

def validate_data(video_df, channel_stats_df):
    class VideoDataSet(PandasDataset):
        pass

    class ChannelDataSet(PandasDataset):
        pass
    """
    Run Great Expectations checks on both video and channel dataframes.
    Raises an error if validation fails.
    """
    # Video data expectations
    video_data = VideoDataSet(video_df)

    video_data.expect_column_to_exist('video_id')
    video_data.expect_column_values_to_not_be_null('video_id')
    video_data.expect_column_values_to_be_unique('video_id')

    video_data.expect_column_values_to_be_between('duration_secs', min_value=0, max_value=86400)
    video_data.expect_column_values_to_be_of_type('view_count', 'int')
    video_data.expect_column_values_to_be_of_type('like_count', 'int')
    video_data.expect_column_values_to_be_of_type('comment_count', 'int')

    video_result = video_data.validate()
    if not video_result['success']:
        raise ValueError("Video data quality checks failed", video_result)

    # Channel data expectations
    channel_data = ChannelDataSet(channel_stats_df)

    channel_data.expect_column_to_exist('channel_name')
    channel_data.expect_column_values_to_not_be_null('channel_name')
    channel_data.expect_column_values_to_be_unique('channel_name')

    

    channel_result = channel_data.validate()
    if not channel_result['success']:
        raise ValueError("Channel stats data quality checks failed", channel_result)

    print("Data quality checks passed successfully.")
    return video_df, channel_stats_df

def load_data(video_df, channel_stats_df):
    """
    Load the transformed data into a SQL Server database.
    Params:
    
    video_df: Transformed DataFrame with video data
    channel_stats_df: Transformed DataFrame with channel statistics
    
    Returns:
    None
    
    """
    params = urllib.parse.quote_plus(
        'DRIVER=ODBC Driver 17 for SQL Server;'
        'SERVER=DESKTOP-E27DIGH\\SQLEXPRESS;'
        'DATABASE=youtube_data_niche;'
        'Trusted_Connection=yes'
    )
    engine = sa.create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
    
    # Encountered 'IntegrityError'. Cannot insert timestapms into MSSQL server.
    video_df = video_df.drop(columns=['published_at'])
    
    video_df.to_sql('videos', con=engine, if_exists='replace',
                    dtype={'video_id': sa.types.VARCHAR(length=40), 
                       'channel_title': sa.types.VARCHAR(),
                       'title': sa.types.VARCHAR(length=200),
                       'description': sa.types.TEXT(),
                       'tags': sa.types.VARCHAR(),
                       'like_count': sa.types.BIGINT(),
                       'favourite_count': sa.types.BIGINT(),
                       'comment_count': sa.types.BIGINT(),
                       'duration': sa.types.VARCHAR(),
                       'definition': sa.types.VARCHAR(),
                       'caption': sa.types.VARCHAR(),
                       'duration_secs': sa.types.BIGINT(),
                       'duration_standard': sa.types.TIME() 
                          })
    
    channel_stats_df.to_sql('channels', con=engine, if_exists='replace', 
                    dtype={'channel_name': sa.types.VARCHAR(length=40), 
                       'subscribers': sa.types.VARCHAR(),
                       'views': sa.types.VARCHAR(length=200),
                       'total_videos': sa.types.TEXT(),
                       'playlist_id': sa.types.VARCHAR()}
                           )
    return 1
  
def main():
    """
    Main function to run the ETL process.
    It extracts data from YouTube API, transforms it, and loads it into a SQL Server database.
    """
    video_df, channel_stats_df = extract_data()
    video_df, channel_stats_df = transform_data(video_df, channel_stats_df)
    validate_data(video_df, channel_stats_df)
    load_data(video_df, channel_stats_df)
    print("ETL process completed successfully.")

if __name__ == "__main__":
    main()
