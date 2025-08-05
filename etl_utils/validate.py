import great_expectations as ge

def validate_data(video_df, channel_stats_df):
    class VideoDataSet(ge.dataset.PandasDataset):
        pass

    class ChannelDataSet(ge.dataset.PandasDataset):
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
