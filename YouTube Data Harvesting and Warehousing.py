import psycopg2
import pandas as pd
import streamlit as st
from googleapiclient.discovery import build
import isodate
from datetime import datetime
import time

# YouTube API setup
API_KEY = 'API'  # Replace with your API key
youtube = build('youtube', 'v3', developerKey=API_KEY)

# Database connection function
def get_db_connection():
    try:
        conn = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="2345",
            database="data",
            port="5432"
        )
        return conn
    except psycopg2.Error as err:
        st.error(f"Error connecting to PostgreSQL: {err}")
        return None

def delete_table_data(table_name):
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute(f"DELETE FROM {table_name}")
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error deleting data: {str(e)}")
        return False
    finally:
        conn.close()

def delete_all_data():
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        # Delete in correct order due to foreign key constraints
        cursor.execute("DELETE FROM comments")
        cursor.execute("DELETE FROM videos")
        cursor.execute("DELETE FROM channels")
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error deleting all data: {str(e)}")
        return False
    finally:
        conn.close()

# YouTube Data Collection Functions
def get_channel_stats(youtube, channel_id):
    try:
        request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
        )
        response = request.execute()

        if not response.get('items'):
            return None

        channel_data = response['items'][0]
        data = {
            'channelName': channel_data['snippet']['title'],
            'channelid': channel_data['id'],
            'subscribers': int(channel_data['statistics']['subscriberCount']),
            'views': int(channel_data['statistics']['viewCount']),
            'totalVideos': int(channel_data['statistics']['videoCount']),
            'playlistId': channel_data['contentDetails']['relatedPlaylists']['uploads'],
            'channel_description': channel_data['snippet']['description']
        }
        return data
    except Exception as e:
        st.error(f"Error fetching channel data: {str(e)}")
        return None

def get_video_ids(youtube, playlist_id):
    try:
        video_ids = []
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50
        )
        response = request.execute()

        for item in response['items']:
            video_ids.append(item['contentDetails']['videoId'])

        next_page_token = response.get('nextPageToken')
        while next_page_token:
            request = youtube.playlistItems().list(
                part='contentDetails',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            for item in response['items']:
                video_ids.append(item['contentDetails']['videoId'])
            next_page_token = response.get('nextPageToken')

        return video_ids
    except Exception as e:
        st.error(f"Error fetching video IDs: {str(e)}")
        return []

def format_duration(duration):
    try:
        duration_obj = isodate.parse_duration(duration)
        hours = duration_obj.total_seconds() // 3600
        minutes = (duration_obj.total_seconds() % 3600) // 60
        seconds = duration_obj.total_seconds() % 60
        return f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"
    except:
        return "00:00:00"

def get_video_details(youtube, video_id):
    try:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()

        if not response['items']:
            return None

        video = response['items'][0]
        
        return {
            'video_id': video['id'],
            'channelid': video['snippet']['channelId'],
            'title': video['snippet']['title'],
            'description': video['snippet']['description'],
            'tags': video['snippet'].get('tags', []),
            'publishedAt': video['snippet']['publishedAt'],
            'viewCount': int(video['statistics'].get('viewCount', 0)),
            'likeCount': int(video['statistics'].get('likeCount', 0)),

            'favoriteCount': int(video['statistics'].get('favoriteCount', 0)),
            'commentCount': int(video['statistics'].get('commentCount', 0)),
            'duration': format_duration(video['contentDetails']['duration']),
            'definition': video['contentDetails']['definition'],
            'caption': video['contentDetails'].get('caption', 'false')
        }
    except Exception as e:
        st.error(f"Error fetching video details: {str(e)}")
        return None

def get_video_comments(youtube, video_id, max_comments=100):
    try:
        comments = []
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=min(max_comments, 100)
        )
        response = request.execute()

        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            comments.append({
                'comment_id': item['id'],
                'video_id': video_id,
                'comment_text': comment['textDisplay'],
                'author_name': comment['authorDisplayName'],
                'published_at': comment['publishedAt']
            })

        return comments
    except:
        return []

# Database Operations
def create_tables():
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Channels table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS channels (
            channel_id VARCHAR(255) PRIMARY KEY,
            channel_name VARCHAR(255),
            subscriber_count BIGINT,
            view_count BIGINT,
            video_count INTEGER,
            playlist_id VARCHAR(255),
            description TEXT
        )
        ''')
        
        # Videos table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS videos (
            video_id VARCHAR(255) PRIMARY KEY,
            channel_id VARCHAR(255) REFERENCES channels(channel_id),
            title VARCHAR(255),
            description TEXT,
            tags TEXT,
            published_at TIMESTAMP,
            view_count BIGINT,
            like_count BIGINT,
            favorite_count INTEGER,
            comment_count INTEGER,
            duration VARCHAR(50),
            definition VARCHAR(50),
            caption VARCHAR(50)
        )
        ''')

        # Comments table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS comments (
            comment_id VARCHAR(255) PRIMARY KEY,
            video_id VARCHAR(255) REFERENCES videos(video_id),
            comment_text TEXT,
            author_name VARCHAR(255),
            published_at TIMESTAMP
        )
        ''')

        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error creating tables: {str(e)}")
        return False
    finally:
        conn.close()

def store_channel_data(channel_data):
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO channels (
                channel_name, channel_id, subscriber_count, 
                view_count, video_count, playlist_id, description
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (channel_id) DO UPDATE SET
                subscriber_count = EXCLUDED.subscriber_count,
                view_count = EXCLUDED.view_count,
                video_count = EXCLUDED.video_count
        ''', (
            channel_data['channelName'],
            channel_data['channelid'],
            channel_data['subscribers'],
            channel_data['views'],
            channel_data['totalVideos'],
            channel_data['playlistId'],
            channel_data['channel_description']
        ))
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error storing channel data: {str(e)}")
        return False
    finally:
        conn.close()

def store_video_data(video_data):
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO videos (
                video_id, channel_id, title, description, tags,
                published_at, view_count, like_count,
                favorite_count, comment_count, duration, definition, caption
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (video_id) DO UPDATE SET
                view_count = EXCLUDED.view_count,
                like_count = EXCLUDED.like_count,
                dislike_count = EXCLUDED.dislike_count,
                comment_count = EXCLUDED.comment_count
        ''', (
            video_data['video_id'],
            video_data['channelid'],
            video_data['title'],
            video_data['description'],
            ','.join(video_data['tags']) if video_data.get('tags') else None,
            video_data['publishedAt'],
            video_data['viewCount'],
            video_data['likeCount'],

            video_data['favoriteCount'],
            video_data['commentCount'],
            video_data['duration'],
            video_data['definition'],
            video_data['caption']
        ))
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error storing video data: {str(e)}")
        return False
    finally:
        conn.close()

def store_comment_data(comment):
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO comments (
                comment_id, video_id, comment_text,
                author_name, published_at
            ) VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (comment_id) DO NOTHING
        ''', (
            comment['comment_id'],
            comment['video_id'],
            comment['comment_text'],
            comment['author_name'],
            comment['published_at']
        ))
        conn.commit()
        return True
    except Exception as e:
        st.error(f"Error storing comment data: {str(e)}")
        return False
    finally:
        conn.close()

def process_channel(channel_id, progress_text):
    try:
        # Get channel data
        progress_text.text(f"Fetching channel data for {channel_id}...")
        channel_data = get_channel_stats(youtube, channel_id)
        if not channel_data:
            st.error(f"Could not fetch data for channel {channel_id}")
            return False

        # Store channel data
        progress_text.text(f"Storing channel information...")
        if not store_channel_data(channel_data):
            st.error("Failed to store channel data")
            return False

        # Get video IDs from channel's upload playlist
        progress_text.text(f"Fetching video list...")
        video_ids = get_video_ids(youtube, channel_data['playlistId'])
        if not video_ids:
            st.warning("No videos found for this channel")
            return True

        total_videos = len(video_ids)
        progress_text.text(f"Processing {total_videos} videos...")

        # Process each video
        for i, video_id in enumerate(video_ids, 1):
            # Get and store video details
            video_data = get_video_details(youtube, video_id)
            if video_data:
                if store_video_data(video_data):
                    # Get and store video comments
                    comments = get_video_comments(youtube, video_id)
                    for comment in comments:
                        store_comment_data(comment)
            
            # Update progress message
            progress_text.text(f"Processed {i}/{total_videos} videos...")

        progress_text.text(f"Completed processing channel {channel_id}")
        return True
    except Exception as e:
        st.error(f"Error processing channel: {str(e)}")
        return False

def execute_analysis_query(query):
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        return pd.read_sql_query(query, conn)
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return None
    finally:
        conn.close()

def get_analysis_query(question):
    queries = {
        "1. What are the names of all the videos and their corresponding channels?": """
            SELECT v.title AS video_name, c.channel_name
            FROM videos v
            JOIN channels c ON v.channel_id = c.channel_id
            ORDER BY c.channel_name, v.title;

        """,
        
        "2. Which channels have the most number of videos, and how many videos do they have?": """
            SELECT c.channel_name, COUNT(v.video_id) as video_count
            FROM channels c
            LEFT JOIN videos v ON c.channel_id = v.channel_id
            GROUP BY c.channel_name
            ORDER BY video_count DESC;
        """,
        
        "3. What are the top 10 most viewed videos and their respective channels?": """
            SELECT v.title AS video_name, c.channel_name, v.view_count
            FROM videos v
            JOIN channels c ON v.channel_id = c.channel_id
            ORDER BY v.view_count DESC
            LIMIT 10;

        """,
        
        "4. How many comments were made on each video, and what are their corresponding video names?": """
            SELECT v.title AS video_name, c.channel_name, v.comment_count
            FROM videos v
            JOIN channels c ON v.channel_id = c.channel_id
            ORDER BY v.comment_count DESC;

        """,
        
        "5. Which videos have the highest number of likes, and what are their corresponding channel names?": """
            SELECT v.title AS video_name, c.channel_name, v.like_count
            FROM videos v
            JOIN channels c ON v.channel_id = c.channel_id
            ORDER BY v.like_count DESC
            LIMIT 10;

        """,
        
        "6. What is the total number of likes for each video, and what are their corresponding video names?": """
            SELECT v.title AS video_name, c.channel_name, v.like_count
            FROM videos v
            JOIN channels c ON v.channel_id = c.channel_id
            ORDER BY v.like_count DESC;

        """,
        
        "7. What is the total number of views for each channel, and what are their corresponding channel names?": """
            SELECT c.channel_name, c.view_count as total_views
            FROM channels c
            ORDER BY c.view_count DESC;
        """,
        
        "8. What are the names of all the channels that have published videos in the year 2022?": """
            WITH yearly_stats AS (
                SELECT 
                    c.channel_name,
                    EXTRACT(YEAR FROM v.published_at) as publish_year,
                    COUNT(*) as video_count,
                    SUM(v.view_count) as total_views,
                    array_agg(v.title) as video_titles
                FROM channels c
                JOIN videos v ON c.channel_id = v.channel_id
                GROUP BY c.channel_name, EXTRACT(YEAR FROM v.published_at)
            )
            SELECT 
                channel_name as "Channel Name",
                publish_year as "Year",
                video_count as "Videos Published",
                total_views as "Total Views",
                video_titles as "Video Titles"
            FROM yearly_stats
            ORDER BY publish_year DESC, video_count DESC;
        """,
        
        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?": """
            SELECT 
                c.channel_name,
                AVG(
                    EXTRACT(EPOCH FROM 
                        INTERVAL '1 second' * (
                            CAST(SPLIT_PART(v.duration, ':', 1) AS INTEGER) * 3600 +
                            CAST(SPLIT_PART(v.duration, ':', 2) AS INTEGER) * 60 +
                            CAST(SPLIT_PART(v.duration, ':', 3) AS INTEGER)
                        )
                    )
                ) / 60 as avg_duration_minutes
            FROM videos v
            JOIN channels c ON v.channel_id = c.channel_id
            GROUP BY c.channel_name
            ORDER BY avg_duration_minutes DESC;
        """,
        
        "10. Which videos have the highest number of comments, and what are their corresponding channel names?": """
            SELECT v.title AS video_name, c.channel_name, v.comment_count
            FROM videos v
            JOIN channels c ON v.channel_id = c.channel_id
            ORDER BY v.comment_count DESC
            LIMIT 10;

        """
    }
    return queries.get(question)

def main():
    st.title("YouTube Data Harvester and Analytics")
    # Create tables if they don't exist
    create_tables()

    # Data Collection Section
    st.header("1. Data Collection")
    channel_id = st.text_input("Enter the Channel IDs here (comma-separated)")
    
    if st.button("Collect and Store Data"):
        if not channel_id:
            st.warning("Please enter at least one channel ID")
            return

        channels = [ch.strip() for ch in channel_id.split(',') if ch.strip()]
        progress_text = st.empty()
        
        for channel in channels:
            progress_text.text(f"Starting to process channel {channel}...")
            success = process_channel(channel, progress_text)
            if success:
                st.success(f"Successfully processed channel {channel}")
            else:
                st.error(f"Failed to process channel {channel}")
        
        progress_text.empty()

    # Data Viewing and Analysis Section
    st.header("2. Data Exploration and Analysis")
    
    tab1, tab2 = st.tabs(["Basic Data View", "Advanced Analysis"])
    
    with tab1:
        col1, col2 = st.columns([4, 1])
        
        with col1:
            table_choice = st.selectbox(
                "Select table to view", 
                ['channels', 'videos', 'comments']
            )
        
        with col2:
            if st.button('🗑️ Delete', key='delete_basic', type='secondary'):
                if delete_table_data(table_choice):
                    st.success(f"Successfully deleted all data from {table_choice}")
                    time.sleep(1)
                    st.rerun()
        
        if table_choice:
            conn = get_db_connection()
            if conn:
                try:
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT * FROM {table_choice} LIMIT 1000")
                    data = cursor.fetchall()
                    
                    if data:
                        columns = [desc[0] for desc in cursor.description]
                        df = pd.DataFrame(data, columns=columns)
                        st.dataframe(df)
                        
                        csv = df.to_csv(index=False)
                        st.download_button(
                            "Download Data",
                            csv,
                            f"{table_choice}_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                            "text/csv",
                            key=f'download-{table_choice}'
                        )
                    else:
                        st.info(f"No data available in {table_choice} table")
                except Exception as e:
                    st.error(f"Error viewing data: {str(e)}")
                finally:
                    conn.close()
    
    with tab2:
        col1, col2 = st.columns([4, 1])
        
        with col1:
            st.subheader("YouTube Data Analysis")
        
        with col2:
            if st.button('🗑️ Delete All', key='delete_advanced', type='secondary'):
                if delete_all_data():
                    st.success("Successfully deleted all data")
                    time.sleep(1)
                    st.rerun()
        
        analysis_question = st.selectbox(
            "Select your question",
            [
                "Select a question...",
                "1. What are the names of all the videos and their corresponding channels?",
                "2. Which channels have the most number of videos, and how many videos do they have?",
                "3. What are the top 10 most viewed videos and their respective channels?",
                "4. How many comments were made on each video, and what are their corresponding video names?",
                "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                "6. What is the total number of likes for each video, and what are their corresponding video names?",
                "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                "8. What are the names of all the channels that have published videos in the year 2022?",
                "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
            ]
        )
        
        if analysis_question != "Select a question...":
            query = get_analysis_query(analysis_question)
            if query:
                df = execute_analysis_query(query)
                if df is not None and not df.empty:
                    st.dataframe(df)
                    
                    csv = df.to_csv(index=False)
                    st.download_button(
                        "Download Results",
                        csv,
                        f"youtube_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        "text/csv",
                        key='download-analysis'
                    )
                    
                    numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
                    if not numeric_cols.empty:
                        st.subheader("Summary Statistics")
                        st.dataframe(df[numeric_cols].describe())
                else:
                    st.info("No data available for this analysis")

if __name__ == "__main__":
    main()
