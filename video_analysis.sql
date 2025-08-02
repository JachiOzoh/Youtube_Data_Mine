/* -----------------------------
 YouTube Video Analysis
 Author: Jachimike F. Ozoh
 Last updated: 2025-08-1
 -----------------------------*/
/*
Engagement Rate
Which video has the highest engagement rate?
*/

SELECT 
  video_id,channel_title,
  ROUND(((like_count + comment_count) * 1.0 / NULLIF(view_count, 0)) * 100,2) AS engagement_rate
FROM videos
ORDER BY engagement_rate DESC;
-- Video ID: FRJlIfqCDek from Data with Baraa has the higest engagement rate with 20.28

/*Optimal Video Length
Does video duration affect views and likes?
*/

WITH video_duration_grouped AS (
  SELECT *,
    CASE 
      WHEN duration_secs < 300 THEN '<5 min'
      WHEN duration_secs BETWEEN 300 AND 900 THEN '5-15 min'
      ELSE '15+ min'
    END AS duration_group
  FROM videos
)
SELECT 
  duration_group,
  COUNT(*) AS total_videos,
  ROUND(AVG(view_count),2) AS avg_views,
  AVG(like_count) AS avg_likes,
  ROUND(AVG(((like_count + comment_count) * 1.0 / NULLIF(view_count, 0)) * 100),2) AS engagement_rate
FROM video_duration_grouped
GROUP BY duration_group
ORDER BY total_videos DESC;
/* Most videos are in the 5-15 min range. Longer form(15+ min) content is getting the most views and likes,
however engagement is higher for short form(<5 min) content.
Possible hypotheses: 1. High view count drags down engagement rate by decreasing the ratio of like and comments to views.
						This means long form content may provide the best value despite what engagement scores say 
					 2. Shorts may be dragging up the engagement rate of <5 min videos.*/

/*
Posting Schedule Impact
Does publish day/time affect performance?
*/
SELECT 
  DATEPART(hour, publish_time) AS publish_hour,
  AVG(view_count) AS avg_views
FROM videos
GROUP BY DATEPART(hour, publish_time)
ORDER BY avg_views DESC;
--Most succesful hours to publish: 19, 17, 18, 20. A clear trend is that times later in the day between 5pm and 8pm are the best times to publish.
SELECT 
  publish_day_name AS publish_day,
  AVG(view_count) AS avg_views
FROM videos
GROUP BY publish_day_name
ORDER BY avg_views DESC;
--Most succesful days to publish: Monday, Friday, Sunday

/*
Most Engaging Topics
Which topics get the highest views on average?
*/

WITH video_type_table AS (SELECT video_id,
  CASE 
    WHEN title LIKE '%power bi%' THEN 'Power BI'
    WHEN title LIKE '%python%' THEN 'Python'
    WHEN title LIKE '%sql%' THEN 'sql'
	WHEN title LIKE '%tableau%' THEN 'tableau'
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
-- Python and SQL are most popular

/*
Most Covered Topics
Which topics are covered most?
*/

WITH video_type_counts_table AS (
    SELECT
        CAST(SUM(CASE WHEN title LIKE '%power bi%' THEN 1 ELSE 0 END) AS FLOAT) AS pbi_count,
        CAST(SUM(CASE WHEN title LIKE '%python%' THEN 1 ELSE 0 END) AS FLOAT) AS python_count,
        CAST(SUM(CASE WHEN title LIKE '%sql%' THEN 1 ELSE 0 END) AS FLOAT)AS sql_count,
        CAST(SUM(CASE WHEN title LIKE '%tableau%' THEN 1 ELSE 0 END) AS FLOAT) AS tableau_count
    FROM videos
),
video_total AS (
    SELECT COUNT(*) AS video_total
    FROM videos
)

SELECT 
    (vt.pbi_count / vt_total.video_total)* 100 AS avg_pbi,
    (vt.python_count / vt_total.video_total)* 100 AS avg_python,
    (vt.sql_count / vt_total.video_total)* 100 AS avg_sql,
    (vt.tableau_count / vt_total.video_total)* 100 AS avg_tableau
FROM video_type_counts_table vt
CROSS JOIN video_total vt_total
-- About 16% of topics are python related 
/*
Channels With The Highest Engagement
Which channel get the highest engagement on average?
*/

SELECT channel_name, total_videos AS total_videos,
AVG(((like_count + comment_count) * 1.0 / NULLIF(view_count, 0)) *100) AS avg_engagement_rate
FROM channels
INNER JOIN videos
ON channel_name = channel_title
GROUP BY channel_name, total_videos 
ORDER BY avg_engagement_rate  DESC

-- Mo Chen with a 4.9556291647547 average engagement rate