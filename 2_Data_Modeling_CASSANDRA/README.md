# Project: Data Modeling with Cassandra
## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to. Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

They'd like a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions, and wish to bring you on the project. Your role is to create a database for this analysis. You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.

## Project Description
In this project, you'll apply what you've learned on data modeling with Cassandra and build an ETL pipeline using Python. To complete the project, you will need to create 3 different tables from the same initial excel file. A specific query will search each table.

## Data Model
### Tables:

#### session_details:
- (name of the artist)     : artist
- (title of the song)      : song_title
- (item number in session) : item_in_session
- (lenght of the song)     : song_length
- Primary keys necessary to have unique rows and answer the query: session_id and item_in_session 
- Query the table: Give me the artist, song title and song's length in the music app history that was heard during a specific sessionId and itemInSession   

#### artist_and_song_by_users:
- (name of the artist)     : artist
- (title of the song)      : song_title
- (item number in session) : item_in_session
- (first name of the user) : f_name
- (last name of the user)  : l_name
- (unique id of each user) : user_id
- (id of the session)      : session_id
- Primary keys necessary to have unique rows and answer the query: session_id, user_id and item_in_session
- Query the table: Give me only the name of artist, song (sorted by itemInSession) and user (first and last name) for a specific userId and sessionId

#### users_by_song:
- (title of the song)      : song_title
- (first name of the user) : f_name
- (last name of the user)  : l_name
- (unique id of each user) : user_id
- Primary keys necessary to have unique rows and answer the query: song_title and user_id
- Query the table: Give me every user name (first and last) in my music app history who listened to a specific song name


