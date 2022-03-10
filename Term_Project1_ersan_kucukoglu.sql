-- --------------------------------------------------------------------------------------------------------------------
-- 												OPERATIONAL LAYER
-- --------------------------------------------------------------------------------------------------------------------
DROP SCHEMA IF EXISTS movie;
CREATE SCHEMA movie;
USE movie;
-- ----------------------------------------------------------
-- CREATING THE MOVIES TABLE
-- ----------------------------------------------------------
	DROP TABLE IF EXISTS movies;
    Create Table movies(
		MovieID int,
        Title VARCHAR(255),
        MPAA_Rating VARCHAR(255),
        Budget VARCHAR(25),
        Gross VARCHAR(25),
        Release_Date DATE,
        Genre VARCHAR(25),
        Runtime INT,
        Rating char(10),
        Rating_Count char(50),
        Summary text,
        PRIMARY KEY (MovieID)
    );
TRUNCATE movies; 
ALTER TABLE movies
MODIFY budget bigint;
ALTER TABLE movies
MODIFY gross bigint; 
-- ---------------------------------------------------------- 
-- LOADING THE DATA INTO MOVIES
-- ----------------------------------------------------------
LOAD DATA INFILE '/tmp/movies.csv' 
INTO TABLE movies
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES
(MovieID,Title,MPAA_Rating,Budget,Gross,Release_Date,Genre,Runtime,Rating,Rating_Count,Summary);

-- ----------------------------------------------------------
-- CREATING THE ACTORS TABLE
-- ----------------------------------------------------------
DROP TABLE IF EXISTS actors;
    Create Table actors(
		ActorID int,
        Name VARCHAR(50),
        Date_of_Birth varchar(12),
        Birth_City VARCHAR(100),
        Birth_Country VARCHAR(100),
        Height_Inches INT,
        Biography text,
        Gender VARCHAR(10),
        Ethnicity VARCHAR(25),
        NetWorth bigint,
        PRIMARY KEY (ActorID)
    );



TRUNCATE actors; 
-- ----------------------------------------------------------
-- LOADING THE DATA INTO ACTORS   
-- ----------------------------------------------------------
LOAD DATA INFILE '/tmp/actors.csv' 
INTO TABLE actors
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES
(ActorID,Name,@Date_of_Birth,Birth_City,Birth_Country,@Height_Inches,Biography,Gender,Ethnicity,@NetWorth)
SET
NetWorth = nullif(@NetWorth, ''),
Date_of_Birth = nullif(@Date_of_Birth, ''),
Height_Inches = nullif(@Height_Inches, '');


-- ----------------------------------------------------------
-- CREATING THE CHARACTERS 
-- ----------------------------------------------------------
DROP TABLE IF EXISTS characters;
    Create Table characters(
		CharacterId int not null auto_increment,
        MovieID int,
		ActorID int,
        Character_Name VARCHAR(50),
        creditOrder int,
        pay int,
        screentime time,
        primary key(CharacterID),
		Foreign key(MovieID) REFERENCES movie.movies(MovieID),
        Foreign key(ActorID) REFERENCES movie.actors(ActorID)
    );
      
    
TRUNCATE characters;   
-- ----------------------------------------------------------
-- LOADING THE DATA INTO CHARACTERS TABLE
-- ----------------------------------------------------------
LOAD DATA INFILE '/tmp/characters.csv' 
INTO TABLE characters
FIELDS TERMINATED BY ';' 
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n' 
IGNORE 1 LINES
(MovieID,ActorID,Character_Name,creditOrder,@pay,@screentime)
SET
pay = nullif(@pay, ''),
screentime = nullif(@screentime, '');
-- --------------------------------------------------------------------------------------------------------------------
-- 												ANALYTICAL LAYER/ETL		
-- --------------------------------------------------------------------------------------------------------------------

-- ----------------------------------------------------------
--  MOVIES PERFORMANCE
-- ----------------------------------------------------------

DROP PROCEDURE IF EXISTS GetMoviesPerformance;

DELIMITER //

CREATE PROCEDURE GetMoviesPerformance()
BEGIN
		DROP TABLE IF EXISTS Movies_Performance;
		CREATE TABLE Movies_Performance AS
SELECT  
		m.MovieID,
        m.title AS Movie_Title,
		m.genre,
        m.MPAA_Rating,
        m.Release_date,
		m.rating AS Movie_Rating,
        m.budget,
        m.gross,
        gross-budget AS Movie_Profit,
		a.ActorID,
                      a.name AS Actor_Name,
                      a.gender,
                      a.Birth_Country,
                      a.Birth_city,
                      a.Date_of_Birth,
                      a.Height_Inches*2.54 AS Height_cm,
                      a.Ethnicity,
                      a.Networth AS NetWorth_$,
                      c.creditOrder,
                      c.Character_name

FROM movies m
LEFT JOIN characters c
using (MovieID)
LEFT JOIN actors a
using(ActorID)

order by movie_rating desc,movie_profit;

END //

DELIMITER ;
Call GetMoviesPerformance;
-- View Movies_Performance Data Warehouse
SELECT * FROM Movies_Performance;

-- ----------------------------------------------------------
-- What is the movie genre ranking list?
-- ----------------------------------------------------------
DROP VIEW IF EXISTS Genre_ranking;
CREATE VIEW `Genre_ranking` AS
SELECT genre,
count(distinct Movie_title) AS Total_Movies,
ROUND(sum(Movie_Rating)/count(Movie_Rating),2) AS avg_rating,
ROUND(sum(movie_profit)/count(movie_profit)) AS Avg_Profit
FROM Movies_Performance
group by genre
order by avg_rating desc;
-- ----------------------------------------------------------
-- What are the top 50 movies by rating?
-- ----------------------------------------------------------
DROP VIEW IF EXISTS Top_50Movies;
CREATE VIEW `Top_50Movies` AS
SELECT 
MovieID,
Movie_Title,
genre,
release_date,
MPAA_Rating,
Movie_Rating,
Movie_Profit
FROM movies_performance
group by movieid
order by Movie_Rating desc,movie_profit desc
Limit 50;

-- ----------------------------------------------------------
-- What are the most popular 20 movies?
-- ----------------------------------------------------------
DROP VIEW IF EXISTS top20_popular_movies;
CREATE VIEW `top20_popular_movies` AS
Select 
	distinct movieID,
	movie_title,
	genre,
	Movie_Rating,
    Movie_Profit
From movies_performance
order by Movie_Profit desc
limit 20;

-- ----------------------------------------------------------
-- Who are the top 10 actors with the highest main role?
-- ----------------------------------------------------------
DROP VIEW IF EXISTS Top_10_Famous_Actors;
CREATE VIEW `Top_10_Famous_Actors` AS
SELECT ActorID,
Actor_Name,
gender,
Ethnicity,
NetWorth_$,
count(creditOrder) AS total_Nof_main_roles
FROM Movies_Performance
where creditOrder=1
GROUP BY ActorID,Actor_Name,gender,Ethnicity,NetWorth_$
ORDER BY total_Nof_main_roles DESC,NetWorth_$ desc
LIMIT 10;









