import urllib
import sqlalchemy as sa

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