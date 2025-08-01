/* -----------------------------
 YouTube Video Analysis
 Author: Jachimike F. Ozoh
 Last updated: 2025-07-30
 -----------------------------*/
/*
Engagement Rate
Which video has the highest engagement rate?
*/
SELECT 
  video_id,channel_title,
  ((like_count + comment_count) * 1.0 / NULLIF(view_count, 0)) * 100 AS engagement_rate
FROM videos
ORDER BY engagement_rate DESC;


/*Optimal Video Length
Does video duration affect views and likes?
*/
WITH video_duration_grouped AS (
  SELECT 
    *,
    CASE 
      WHEN duration_secs < 300 THEN '<5 min'
      WHEN duration_secs BETWEEN 300 AND 900 THEN '5-15 min'
      ELSE '15+ min'
    END AS duration_group
  FROM videos
)

SELECT 
  duration_group,
  ROUND(AVG(view_count),2) AS avg_views,
  AVG(like_count) AS avg_likes
FROM video_duration_grouped
GROUP BY duration_group
ORDER BY avg_views DESC;


/*
Posting Schedule Impact
Does publish day/time affect performance?
*/
SELECT 
  publish_day_name AS publish_day,
  DATEPART(hour, publish_time) AS publish_hour,
  AVG(view_count) AS avg_views
FROM videos
GROUP BY publish_day_name, DATEPART(hour, publish_time)
ORDER BY avg_views DESC;

/*
Most Engaging Topics
Which topic get the highest views on average?
*/

WITH video_type_table AS (SELECT video_id,
  CASE 
    WHEN title LIKE '%power bi%' THEN 'Power BI'
    WHEN title LIKE '%python%' THEN 'Python'
    WHEN title LIKE '%sql%' THEN 'sql'
    ELSE 'Other'
  END AS content_type
FROM videos)

SELECT vt.content_type AS content_type, AVG(v.view_count) AS average_views
FROM videos v
INNER JOIN video_type_table vt
ON v.video_id=vt.video_id
WHERE vt.content_type NOT LIKE 'Other'
GROUP BY vt.content_type
ORDER BY average_views DESC



SELECT channel_name, CAST(total_videos AS varchar(max)) AS total_videos,
AVG(((like_count + comment_count) * 1.0 / NULLIF(view_count, 0)) *100) AS avg_engagement_rate
FROM channels
INNER JOIN videos
ON channel_name = channel_title
GROUP BY channel_name, CAST(total_videos AS varchar(max))
ORDER BY CAST(total_videos AS varchar(max)) DESC
