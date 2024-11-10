import pandas as pd
from googleapiclient.discovery import build
import streamlit as st
import mysql.connector
import datetime

# Set up API connection details
API_KEY = "AIzaSyA0huyndJmXmerYM0fW8WpiDng_kpy2vcE"  # Replace with your actual API key
API_NAME = "youtube"
API_VERSION = "v3"

# MySQL connection configuration
mysql_host = "localhost"
mysql_user = "root"
mysql_password = "AI@Guvi12345"  # Replace with your MySQL password
mysql_database = "youtube_data"
mysql_port = "3306"

# Connect to YouTube API
def api_connect():
    try:
        youtube = build(API_NAME, API_VERSION, developerKey=API_KEY)
        return youtube
    except Exception as e:
        st.error(f"Error connecting to YouTube API: {e}")
        return None

# Connect to MySQL database with utf8mb4 character set for emoji support
def connect_to_mysql():
    try:
        conn = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database,
            port=mysql_port,
            auth_plugin='mysql_native_password',
            charset='utf8mb4'
        )
        st.success("Connected to MySQL database successfully")
        return conn
    except mysql.connector.Error as e:
        st.error(f"Error connecting to MySQL database: {e}")
        return None

# Create tables with utf8mb4 encoding
def create_tables(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS channel_data (
            Channel_Name VARCHAR(255) CHARACTER SET utf8mb4,
            Channel_Id VARCHAR(255) PRIMARY KEY,
            Subscribers INT,
            Views INT,
            Total_videos INT,
            Channel_description TEXT CHARACTER SET utf8mb4,
            Playlist_Id VARCHAR(255)
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS video_data (
            Channel_Name VARCHAR(255) CHARACTER SET utf8mb4,
            Channel_Id VARCHAR(255),
            Video_Id VARCHAR(255) PRIMARY KEY,
            Title VARCHAR(255) CHARACTER SET utf8mb4,
            Tags TEXT CHARACTER SET utf8mb4,
            Thumbnail TEXT CHARACTER SET utf8mb4,
            Description TEXT CHARACTER SET utf8mb4,
            Publishdate DATETIME,
            Duration VARCHAR(255),
            Views INT,
            Likes INT,
            Comments INT,
            Favorite_count INT,
            Definition VARCHAR(255),
            Caption_Status VARCHAR(255)
        )
    """)
    conn.commit()
    cursor.close()

# Retrieve and insert channel data
def get_channel_info(youtube, channel_id):
    request = youtube.channels().list(part="snippet,statistics,contentDetails", id=channel_id)
    response = request.execute()
    data = []
    for item in response["items"]:
        data.append({
            "Channel_Name": item["snippet"]["title"],
            "Channel_Id": item["id"],
            "Subscribers": int(item["statistics"]["subscriberCount"]),
            "Views": int(item["statistics"]["viewCount"]),
            "Total_videos": int(item["statistics"]["videoCount"]),
            "Channel_description": item["snippet"]["description"],
            "Playlist_Id": item["contentDetails"]["relatedPlaylists"]["uploads"]
        })
    return data

# Retrieve video IDs from a playlist
def get_video_ids(youtube, playlist_id):
    video_ids = []
    next_page_token = None
    while True:
        response = youtube.playlistItems().list(part="snippet", playlistId=playlist_id, maxResults=50, pageToken=next_page_token).execute()
        video_ids.extend([item['snippet']['resourceId']['videoId'] for item in response['items']])
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
    return video_ids

# Retrieve video details with datetime conversion
def get_video_details(youtube, video_ids):
    video_data = []
    for video_id in video_ids:
        response = youtube.videos().list(part="snippet,contentDetails,statistics", id=video_id).execute()
        for item in response["items"]:
            publish_date_str = item['snippet']['publishedAt']
            publish_date = datetime.datetime.strptime(publish_date_str, '%Y-%m-%dT%H:%M:%SZ')
            formatted_publish_date = publish_date.strftime('%Y-%m-%d %H:%M:%S')
            
            data = {
                'Channel_Name': item['snippet']['channelTitle'],
                'Channel_Id': item['snippet']['channelId'],
                'Video_Id': item['id'],
                'Title': item['snippet']['title'],
                'Tags': ', '.join(item['snippet'].get('tags', [])),
                'Thumbnail': item['snippet']['thumbnails']['default']['url'],
                'Description': item['snippet'].get('description', ''),
                'Publishdate': formatted_publish_date,
                'Duration': item['contentDetails']['duration'],
                'Views': int(item['statistics'].get('viewCount', 0)),
                'Likes': int(item['statistics'].get('likeCount', 0)),
                'Comments': int(item['statistics'].get('commentCount', 0)),
                'Favorite_count': int(item['statistics'].get('favoriteCount', 0)),
                'Definition': item['contentDetails'].get('definition', ''),
                'Caption_Status': item['contentDetails'].get('caption', '')
            }
            video_data.append(data)
    return video_data

# Insert data into MySQL
def insert_data(conn, data, table_name):
    cursor = conn.cursor()
    if table_name == "channel_data":
        for channel in data:
            cursor.execute("""
            INSERT INTO channel_data (Channel_Name, Channel_Id, Subscribers, Views, Total_videos, Channel_description, Playlist_Id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Subscribers=VALUES(Subscribers),
                Views=VALUES(Views),
                Total_videos=VALUES(Total_videos),
                Channel_description=VALUES(Channel_description)
            """, (
                channel['Channel_Name'], channel['Channel_Id'], channel['Subscribers'], channel['Views'],
                channel['Total_videos'], channel['Channel_description'], channel['Playlist_Id']
            ))
    elif table_name == "video_data":
        for video in data:
            cursor.execute("""
            INSERT INTO video_data (Channel_Name, Channel_Id, Video_Id, Title, Tags, Thumbnail, Description, Publishdate, Duration, Views, Likes, Comments, Favorite_count, Definition, Caption_Status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Views=VALUES(Views),
                Likes=VALUES(Likes),
                Comments=VALUES(Comments),
                Favorite_count=VALUES(Favorite_count),
                Definition=VALUES(Definition),
                Caption_Status=VALUES(Caption_Status)
            """, (
                video['Channel_Name'], video['Channel_Id'], video['Video_Id'], video['Title'], video['Tags'],
                video['Thumbnail'], video['Description'], video['Publishdate'], video['Duration'], video['Views'],
                video['Likes'], video['Comments'], video['Favorite_count'], video['Definition'], video['Caption_Status']
            ))
    conn.commit()
    cursor.close()

# Delete all data from MySQL tables
def delete_all_data(conn):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM channel_data")
    cursor.execute("DELETE FROM video_data")
    conn.commit()
    cursor.close()

# Streamlit UI for data input and display
def main():
    st.title("YouTube Data Harvesting and Warehousing Application")

    # Accordion layout with Streamlit expanders
    with st.expander("Project Overview"):
        st.write("""
            **Project Title**: YouTube Data Harvesting and Warehousing using SQL and Streamlit  
            **Skills Takeaway**: Python scripting, Data Collection, Streamlit, API Integration, Data Management using SQL  
            **Domain**: Social Media
        """)
    
    with st.expander("Problem Statement and Approach"):
        st.write("""
            **Problem Statement**:  
            Create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.

            **Approach**:  
            1. Set up Streamlit app for data input.  
            2. Connect to YouTube API.  
            3. Store and clean data before warehousing.  
            4. Migrate to SQL data warehouse.  
            5. Use SQL queries for analysis.  
            6. Display data in Streamlit.
        """)

    # Input for YouTube Channel ID
    channel_id = st.text_input("Enter YouTube Channel ID:")
    
    if st.button("Harvest Data"):
        youtube = api_connect()
        conn = connect_to_mysql()

        if youtube and conn:
            create_tables(conn)

            st.info("Fetching channel info...")
            channel_info = get_channel_info(youtube, channel_id)
            if channel_info:
                insert_data(conn, channel_info, "channel_data")
                
                playlist_id = channel_info[0]["Playlist_Id"]
                video_ids = get_video_ids(youtube, playlist_id)
                st.info(f"Found {len(video_ids)} videos. Fetching video details...")

                video_details = get_video_details(youtube, video_ids)
                if video_details:
                    insert_data(conn, video_details, "video_data")
                    st.success("Data harvested and stored successfully in MySQL.")
                else:
                    st.warning("No videos found for this channel.")
            else:
                st.warning("No channel information found.")
    
    # Button to delete all data
    if st.button("Delete All Data"):
        conn = connect_to_mysql()
        if conn:
            delete_all_data(conn)
            st.success("All data has been deleted from the database.")

    # Options to view data
    table_option = st.selectbox("Select Table to View", ["channel_data", "video_data"])
    if table_option:
        conn = connect_to_mysql()
        if conn:
            cursor = conn.cursor()
            cursor.execute(f"SELECT * FROM {table_option}")
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])
            st.write(df)

if __name__ == "__main__":
    main()

