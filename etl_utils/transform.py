import isodate
import pandas as pd

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