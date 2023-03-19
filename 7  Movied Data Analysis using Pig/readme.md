Movie data Set Analysis using PigC.V.Raman Global University
Bhubaneswar,Odisha
Problem Statement: Find the movie with avg rating >4.0 from
u.data dataset.
Step 1: Copy the files form local system Download folder to
root directory:
[root@quickstart cloudera]# hdfs dfs -copyFromLocal
/home/cloudera/Downloads/u.data /
[root@quickstart cloudera]# hdfs dfs -copyFromLocal
/home/cloudera/Downloads/u.item /
And to display do
[root@quickstart cloudera]# hdfs dfs -ls /
Step 2. Load the movie data into pig
Note: Now, open the grunt shell of Apache pig
ratings = LOAD '/u.data' AS (userID:int, movieID:int, rating:int,
ratingTime:int);
dump ratings
Step 3: Find the movie with avg rating >4.0 from u.data
dataset.
To find we follow the following steps
Step 3a) Group the data according to movieID:
ratingsByMovie = GROUP ratings BY movieID;
dump ratingsByMovie;
Step 4) Find average ratings for the grouped data:
avgRatings = FOREACH ratingsByMovie GENERATE group AS movieID,
AVG(ratings.rating) AS avgRating;
dump avgRatings;
Step 5: Filter the required results:
fiveStarMovies = FILTER avgRatings BY avgRating > 4.0;
To print execute:
dump fiveStarMovies;
……………………………………………………………………………………………………………………………………. Problem 2: Find the oldest 5-star movies from u.data and
u.item datasets. • Load the u.item in pig:
details = LOAD '/u.item' USING PigStorage('|') AS (movieID:int,
movieTitle:chararray, releaseDate:chararray, videoRelease:chararray,
imdbLink:chararray);
dump details
• Create a scalable timestamp column to compare the time:
 lookupTable = FOREACH details GENERATE movieID, movieTitle,
ToUnixTime(ToDate(releaseDate, 'dd-MMM-yyyy')) AS releaseTime;
dump lookupTable;
• Filter the movies with average rating as 5:
fiveStarMoviesNew = FILTER avgRatings BY avgRating == 5.0;
dump fiveStarMoviesNew;
• Join this table with the lookup table:
fiveStarsWithDetails = JOIN fiveStarMoviesNew BY movieID, lookupTable
BY movieID;
dump fiveStarsWithDetails;
• Order the results by time (year):
oldestFiveStarMovies = ORDER fiveStarsWithDetails BY
lookupTable::releaseTime;
dump oldestFiveStarMovies;
c). Find the oldest 3-star movies from u.data and u.item datasets.
To do follow the following steps
 Filter the movies with average rating 3:
threeStarMovies = FILTER avgRatings BY avgRating == 3.0;
dump threeStarMovies;
 Join this table with the lookup table found in part (b):
threeStarsWithDetails = JOIN threeStarMovies BY movieID, lookupTable BY
movieID;
dump threeStarsWithDetails;
3) Order the results by time (year):
oldestThreeStarMovies = ORDER threeStarsWithDetails BY
lookupTable::releaseTime;
dump oldestThreeStarMovies;
d). Display name of all movies in uppercase.
If while loading the data we don’t use USING PigStorage(‘,) then while
executing the following commands entire columns would be uppercase as
it is reqd for a delimiter!
 upperNameMovies = FOREACH details GENERATE UPPER(movieTitle);
dump upperNameMovies;
