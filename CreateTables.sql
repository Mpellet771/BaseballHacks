-- create a new database schema for the box score information, 
-- and a user to access the database
GRANT ALL ON boxes.* to 'boxers'@'localhost'
	IDENTIFIED BY 'boxer password';
CREATE DATABASE boxes;
USE boxes;

-- Create three tables containing information about players:
-- batting, fielding, and pitching. Fields are sized to 
-- minimize storage space for this database

CREATE TABLE batting (
       eliasID      INT(6),
       teamID       CHAR(3),
       gameID       VARCHAR(32),
       gameDate     DATE,
       h            SMALLINT(2), -- hits
       hr           SMALLINT(2), -- home runs
       bb           SMALLINT(2), -- walks
       so           SMALLINT(2), -- strikeouts
       rbi          SMALLINT(2), -- runs batted in
       ab           SMALLINT(2), -- at bats
       runs         SMALLINT(2), -- runs scored
       t            SMALLINT(2), -- triples
       d            SMALLINT(2), -- doubles
       lob          SMALLINT(2), -- left on base
       sb           SMALLINT(2), -- stolen bases
       cs           SMALLINT(2)  -- caught stealing
);

CREATE TABLE fielding (
       eliasID      INT(6),
       teamID       CHAR(3),
       gameID       VARCHAR(32),
       gameDate     DATE,
       pos          CHAR(2),     -- position
       po           SMALLINT(2), -- put outs
       da           SMALLINT(2), -- assists
       e            SMALLINT(2), -- errors
       pb           SMALLINT(2)  -- passed balls
);


CREATE TABLE pitching (
       eliasID      INT(6),
       teamID       VARCHAR(3),
       gameID       VARCHAR(32),
       gameDate     DATE,
       outs         SMALLINT(2),
       bf           SMALLINT(2), -- batters faced
       hr           SMALLINT(2), -- home runs
       bb           SMALLINT(2), -- walks
       so           SMALLINT(2), -- strikeouts
       er           SMALLINT(2), -- earned runs
       runs         SMALLINT(2), -- runs
       hits         SMALLINT(2), -- hits
       wins         SMALLINT(2), -- wins
       losses       SMALLINT(2), -- losses
       holds        SMALLINT(2), -- holds
       hopps        SMALLINT(2), -- hold opportunities
       saves        SMALLINT(2), -- saves
       sopps        SMALLINT(2)  -- save opportunities         
);
-- Add a table to keep track of a little information 
-- about each game, such as the opponents and date.

CREATE TABLE games (
       gameID       VARCHAR(32),
       gameDate     DATE,
       homeTeam     CHAR(3),
       awayTeam     CHAR(3)
);

-- Keep track of the complete daily rosters. This 
-- table will include detailed information about 
-- who played in each game.

CREATE TABLE rosters (
       gameID      VARCHAR(32),
       gameDate    DATE,
       teamID      CHAR(3),
       eliasID     INT(6),
       nameFirst   VARCHAR(32),
       nameLast    VARCHAR(32),
       nickName    VARCHAR(32),
       jersey      SMALLINT(2),
       throws      CHAR(1)
);           

-- Show what we¢ve created.
show tables;
