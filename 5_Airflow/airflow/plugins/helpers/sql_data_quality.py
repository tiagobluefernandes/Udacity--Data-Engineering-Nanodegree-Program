class DataQualityQueries:
    table_staging_songs_not_empty=("""
    SELECT (CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END) as result
    FROM public.staging_songs
    """)
        
    table_staging_events_not_empty=("""
    SELECT (CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END) as result
        FROM public.staging_events
    """)
        
    table_staging_songs_not_empty_col_song_id=("""
    SELECT (CASE WHEN COUNT(song_id) > 0 THEN 0 ELSE 1 END) as result FROM public.staging_songs WHERE song_id IS NOT NULL
    """)

    table_staging_songs_not_empty_col_artist_id=("""
    SELECT (CASE WHEN COUNT(artist_id) > 0 THEN 0 ELSE 1 END) as result FROM public.staging_songs WHERE artist_id IS NOT NULL
    """)
    
