# YOUTUBE-DATA-HARVESTING-AND-WAREHOUSING
## Project Overview
This project involves the harvesting and warehousing of YouTube data using Python, Google API, MySQL, and Streamlit. The application allows users to connect to the YouTube API to fetch channel and video data, store this data in an SQL database, and visualize it via a Streamlit dashboard.

## Skills Takeaway
- Python scripting
- Data collection and manipulation
- API integration (YouTube API)
- Data management using SQL
- Data visualization with Streamlit

## Domain
Social Media Data Analysis

---

## Features
- **Harvest Channel Data**: Fetches details about a specific YouTube channel (e.g., name, ID, subscribers, total views, and videos).
- **Harvest Video Data**: Retrieves video details such as title, tags, description, publish date, views, likes, comments, and more.
- **SQL Integration**: Stores channel and video data in MySQL, with options to delete all data or view stored records.
- **Streamlit Interface**: Provides an easy-to-use interface for interacting with the YouTube API, viewing data, and displaying results.

---

## Prerequisites

Before running the application, ensure you have the following installed:

- Python 3.7 or higher
- MySQL Server
- Streamlit
- `google-api-python-client` library
- `mysql-connector-python` library

You can install the necessary libraries using `pip`:

```bash
pip install google-api-python-client mysql-connector-python streamlit pandas
```

---

## Setup Instructions

### 1. Create YouTube API Key
- Go to the [Google Cloud Console](https://console.cloud.google.com/).
- Create a new project.
- Enable the YouTube Data API v3 for the project.
- Create an API key and replace it in the `API_KEY` variable in the script.

### 2. Set Up MySQL Database
- Install MySQL Server on your system if not already installed.
- Create a new database `youtube_data` in MySQL.
- Ensure your MySQL server is running and accessible on the specified host and port.

### 3. Modify MySQL Credentials
Update the MySQL connection details (`mysql_host`, `mysql_user`, `mysql_password`, `mysql_database`, `mysql_port`) in the script with your database credentials.

### 4. Run the Application
Once the setup is complete, run the Streamlit app:

```bash
streamlit run app.py
```

This will open a browser window where you can enter a YouTube channel ID, fetch channel and video data, and store it in the database.

---

## Streamlit Interface

### Sections in the Streamlit App:
1. **Project Overview**: Brief description of the project.
2. **Problem Statement and Approach**: High-level overview of the approach for harvesting and storing data.
3. **Input Fields**:
   - **YouTube Channel ID**: Enter the YouTube channel ID to harvest data.
4. **Buttons**:
   - **Harvest Data**: Fetches channel and video data and stores it in the MySQL database.
   - **Delete All Data**: Deletes all data from the database tables.
5. **Data View**: Option to view data from the `channel_data` or `video_data` table.

---

## Functions in the Script

- **api_connect()**: Connects to the YouTube API using the provided API key.
- **connect_to_mysql()**: Connects to the MySQL database using the provided credentials.
- **create_tables()**: Creates the necessary tables in MySQL (`channel_data` and `video_data`) with utf8mb4 encoding for emoji support.
- **get_channel_info()**: Fetches data about a specific YouTube channel.
- **get_video_ids()**: Retrieves video IDs from a channel's playlist.
- **get_video_details()**: Fetches details for each video in the playlist.
- **insert_data()**: Inserts harvested data into the MySQL database.
- **delete_all_data()**: Deletes all data from the database tables.
- **main()**: The Streamlit app function that handles the UI and connects all functions together.

---

## Troubleshooting

- Ensure that you have the correct YouTube API key and MySQL credentials.
- If the data isn't being inserted, check the MySQL error logs for potential issues with database connections or table creation.
