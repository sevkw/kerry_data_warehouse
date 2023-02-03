union_record_counts = ("""
    SELECT
        'staging_events' AS source
        , COUNT(*)
    FROM public.staging_events
    GROUP BY 1

    UNION 

    SELECT
        'staging_songs' AS source
        , COUNT(*)
    FROM public.staging_songs
    GROUP BY 1

    UNION 

    SELECT
        'fct_songplays' AS source
        , COUNT(*)
    FROM public.fct_songplays
    GROUP BY 1

    UNION

    SELECT
        'dim_users' AS source
        , COUNT(*)
    FROM public.dim_users
    GROUP BY 1

    UNION

    SELECT
        'dim_time' AS source
        , COUNT(*)
    FROM public.dim_time
    GROUP BY 1

    UNION 

    SELECT
        'dim_songs' AS source
        , COUNT(*)
    FROM public.dim_songs
    GROUP BY 1

    UNION

    SELECT
        'dim_artists' AS source
        , COUNT(*)
    FROM public.dim_artists
    GROUP BY 1
""")

weekly_user_avg_play = ("""
    WITH base AS (
    SELECT 
        sp.*
        , time.week
        , time.weekday

    FROM public.fct_songplays AS sp
    LEFT JOIN public.dim_time AS time
      ON sp.start_time = time.start_time
    )
SELECT 
    week
    , level
    , COUNT(songplay_id) AS count_songplays
    , COUNT(DISTINCT user_id) AS count_users
    , COUNT(songplay_id) / COUNT(DISTINCT user_id) songplay_per_user
FROM base
GROUP BY 1,2
ORDER BY week DESC
""")

level_song_play_by_artist = ("""
WITH base AS (
SELECT 
    sp.*
    , a.artist_name

FROM public.fct_songplays AS sp
LEFT JOIN public.dim_artists AS a
  ON sp.artist_id = a.artist_id
WHERE a.artist_name IS NOT NULL
)

SELECT
  artist_name
  , level
  , COUNT(songplay_id) AS count_plays
 FROM base
 GROUP BY 1,2
 ORDER BY 3 DESC
 LIMIT 10
""")
