# Building Data Warehouse

## Introduction

Sparkify wants a Data Engineer to build an ETL pipeline that extracts data from S3, stages into Redshift, and transform data into set of dimensional and fact tables for analytics use cases.

## DataSets

Location of datasets that resides in S3

- Song data: `s3://udacity-dend/song_data`
- Log data: `s3://udacity-dend/log_data`

## Schema for Song Play Analysis

#### Dimensional Tables

- users
    - `user_id`, `first_name`, `last_name`, `gender`, `level`
- songs
    - `song_id`, `title`, `artist_id`, `year`, `duration`
- artists
    - `artist_id`, `name`, `location`, `latitude`, `longitude`
- time
    - `start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday`

#### Fact Table

- songplays
    - `songplay_id`, `start_time`, `user_id`, `level`, `song_id`, `artist_id`, `session_id`, `location`, `user_agent`



