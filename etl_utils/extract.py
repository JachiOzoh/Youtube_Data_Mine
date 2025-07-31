import googleapiclient.discovery
import pandas as pd
import isodate
import os

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


def extract_data():
    """
    Extract data from YouTube API for a list of channels
    Params:
    None
    
    Returns:
    video_df: DataFrame with video data
    channel_stats_df: DataFrame with channel statistics
    
    """
    
    
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
