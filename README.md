# Data-Warehouse-with-Spark

# Summary

The goal of this project is to create a database schema and ETL pipeline with AWS data lake service. <br>
The dataset is comprised of two different datasets called log and song datasets reside in S3 buckets.<br> 
The first dataset is called **song_data**, which is a subset of real data from the Million Song Dataset. <br> 
Each file of the **song_data** dataset is in JSON format and contains metadata about a song and the artist of that song.<br>
The second dataset is called **log_data** consists of log files in JSON files, where each file covers the users activities over a given day.<br>

# Database Schema

To reach the goal of the project, we build a star schema optimized for queries on the given dataset. <br>
We create the fact and dimenson tables as follows: <br>
The fact table is **songplays** - records in log data associated with song plays i.e. records with page nextsong.<br>
  - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent <br>

The dimension tables are: <br> 

**users** - users in the app <br>
   - user_id, first_name, last_name, gender, level
   
  
**songs** - songs in music database <br>
   - songs - songs in music database
   
**artists** - artists in music database <br>
   - artist_id, name, location, latitude, longitude
   
**time** - timestamps of records in songplays broken down into specific units <br>
   - start_time, hour, day, week, month, year, weekday
 
 
 # How to Run
 
 1.Add your AWS IAM Credentials in **dl.cfg**
 
 2.Determine your desired output data path in etl.py
 
 3.run **etl.py**
