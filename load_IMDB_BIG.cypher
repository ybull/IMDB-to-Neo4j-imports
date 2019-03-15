/* Cypher script for loading IMDB into Neo4j

Created to use with our IS490DA class.
March 2019

Important TIPS:

1. This script could theoretically be run all at once, but you should run it
     one section at a time and check what it's doing.
2. Most of the LOAD CSV statements below have a LIMIT clause so that you can
     quickly try them on a few lines of the data file.  When ready, comment
     out the LIMIT n clause and re-execute that part so it will load all the
     nodes or relationships it's supposed to.
3. Some of the IMDB TSV files have improper quotes that LOAD CSV chokes on.
     I will provide a Python script that can rewrite them as TSVs with
     compatible quote delimiters. Those files are all called "fixed..." below
4. The file:///... paths below are RELATIVE to your Neo4j
     project's 'IMPORT' directory, unless you decide to change the
     dbms.directories.import configuration setting.
5. PROFILE each big query first before you run it, especially if you
     modified it!  Read the section EAGER LOADING for why.
6. If you load all this, it will take well over 20GB of disk space as a graph
     database. A section below and the in-class discussion will help you decide
     and learn how you can modify the LOAD CSV statements according to your
     preferences.


** EAGER LOADING

As I worked on developing this loading script, I noticed in some cases, the
Neo4j Browser had a small warning about 'eager loading'.  Some of the LOAD CSV
statements would work anyway, but the largest ones ran for a couple hours and
still I had to kill them!

That wasn't explained well in the official documentation but I found a few
blog posts about it.  The best one was this:
https://markhneedham.com/blog/2014/10/23/neo4j-cypher-avoiding-the-eager/

We'll look at this behavior in class. The problem is if ANY operation in
the query plan uses "eager loading" then it will override the "PERIODIC COMMIT",
converting the query into one potentially gigantic transaction.


** IF YOU WANT TO LIMIT THE PORTION(s) of IMDB that you load
to reduce the storage required and graph complexity, there are many
reasonable ways, depending on what parts of IMDB you want to explore.

The simplest idea is to load only certain "title" types that you want, then
load "crew" as Person nodes, potentially filtering those by primaryProfession,
and only load the types of relationships you want.  When creating relationships
make sure it does MATCH existing nodes first before MERGE or CREATE of a
relationship so that you don't accidentally create the unwanted nodes in that
step.

Another way to filter titles is to first load the IMDB RATINGS, to create
title nodes for only those that have a minimum threshold of votes. Notice that
the script is already excluding the 'adult' titles below, and you can add more
conditions in the WHERE clause.
*/


// load the title.basics.tsv file (after quote-correction)   ~ 357 seconds

// tconst  titleType       primaryTitle    originalTitle   isAdult startYear       endYear runtimeMinutes  genres
// tt0000001       short   Carmencita      Carmencita      0       1894    \N      1       Documentary,Short

CREATE CONSTRAINT ON (c:Prod) ASSERT c.tconst IS UNIQUE
 
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM 'file:///IMDB/fixed.title.basics.tsv'
 AS line FIELDTERMINATOR '\t'
 WITH line                            LIMIT 20
 WHERE line.isAdult = '0'  // skip the adult productions
 MERGE (n:Prod {tconst: line.tconst})  // create node if doesn't exist yet
   SET n.titleType =      split(line.titleType,','),
 	   n.title =          line.primaryTitle,
 	   n.originalTitle =  line.originalTitle,
 	   n.startYear =      toInteger(line.startYear),
 	   n.endYear =        toInteger(line.endYear),
 	   n.runtimeMinutes = toInteger(line.runtimeMinutes),
 	   n.genres =         split(line.genres,',')
 

// load the title.episode.tsv  (no modifications needed)    ~ 206 seconds

// tconst  parentTconst    seasonNumber    episodeNumber
// tt0041951       tt0041038       1       9
// tt0042816       tt0989125       1       17
// tt0042889       tt0989125       \N      \N
// tt0043426       tt0040051       3       42

USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM 'file:///IMDB/title.episode.tsv'
 AS line FIELDTERMINATOR '\t'
 WITH line                          LIMIT 20
   // search for both titles listed in the row:
 MATCH (t:Prod {tconst: line.tconst}), (series:Prod {tconst: line.parentTconst})
  WHERE exists(t.tconst) AND exists(series.tconst)  // were they found?
 MERGE (t)-[ep:EPISODE_OF]->(series)  // create relationship if doesn't exist yet
 SET   ep.season = toInteger(line.seasonNumber),
       ep.number = toInteger(line.episodeNumber),
       t:Episode,     // add label :Episode
       series:Series  // add label :Series
       

// load the name.basics.tsv file (after quote-correction)   ~ 521 seconds

// nconst  primaryName     birthYear       deathYear       primaryProfession       knownForTitles
// nm0000001       Fred Astaire    1899    1987    soundtrack,actor,miscellaneous  tt0043044,tt0053137,tt0072308,tt0050419

CREATE CONSTRAINT ON (p:Person) ASSERT p.nconst IS UNIQUE

USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM 'file:///IMDB/fixed.name.basics.tsv'
 AS line FIELDTERMINATOR '\t'
 WITH line                    LIMIT 20
 MERGE (n:Person {nconst: line.nconst})    // create node if doesn't exist yet
 SET 	n.name = 				line.primaryName,
 		n.birthYear =			toInteger(line.birthYear),
 		n.deathYear =			toInteger(line.deathYear),
 		n.primaryProfession =	split(line.primaryProfession, ',')
 	   
       
// load the fixed.title.principals.tsv IN PARTS to avoid 'EAGER LOADING'

// tconst	ordering	nconst	category	job	characters
// tt0000001	1	nm1588970	self	\N	["Herself"]
// tt0000001	2	nm0005690	director	\N	\N
// tt0000001	3	nm0374658	cinematographer	director of photography	\N
       
       
//  Create the :ACTED_IN relationships.    Took mine ~713 seconds       
// PROFILE 
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM 'file:///IMDB/fixed.title.principals.tsv'
 AS line FIELDTERMINATOR '\t'
 WITH line                         LIMIT 200
 MATCH (p:Person {nconst: line.nconst})
 MATCH (t:Prod {tconst: line.tconst})
  WHERE exists(p.nconst) AND exists(t.tconst)
  		AND line.category IN ['actor','actress']
// Add the relationship type needed, based on category:
MERGE (p)-[r:ACTED_IN]->(t)  // create relationship if doesn't exist yet
	ON CREATE SET 	r.order = line.ordering, r.characters = line.characters
	ON MATCH SET 	r.order = line.ordering, r.characters = line.characters
       
//  Create the :DIRECTED relationships.    Took mine ~ 356 seconds       
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM 'file:///IMDB/fixed.title.principals.tsv'
 AS line FIELDTERMINATOR '\t'
 WITH line                         LIMIT 200
 MATCH (p:Person {nconst: line.nconst})
 MATCH (t:Prod {tconst: line.tconst})
  WHERE exists(p.nconst) AND exists(t.tconst)
  		AND line.category = 'director'
// Add the relationship type needed, based on category:
MERGE (p)-[r:DIRECTED]->(t)  // create relationship if doesn't exist yet
	ON CREATE SET r.order = line.ordering, r.job = line.job
	ON MATCH SET  r.order = line.ordering, r.job = line.job
    
//  Create the :WRITER_OF relationships.    Took mine ~ 323 seconds       
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM 'file:///IMDB/fixed.title.principals.tsv'
 AS line FIELDTERMINATOR '\t'
 WITH line                         LIMIT 200
 MATCH (p:Person {nconst: line.nconst})
 MATCH (t:Prod {tconst: line.tconst})
  WHERE exists(p.nconst) AND exists(t.tconst)
  		AND line.category = 'writer'
// Add the relationship type needed, based on category:
MERGE (p)-[r:WRITER_OF]->(t)  // create relationship if doesn't exist yet
	ON CREATE SET r.order = line.ordering, r.job = line.job
	ON MATCH SET  r.order = line.ordering, r.job = line.job
        

/*  OTHER DATA WE COULD LOAD...

There are ratings for titles.

Re-load fixed.name.basics.tsv file to split "knownForTitles" into proper
relationships, or possibly just 'upgrade' those existing relationships with
another label or property

There are some other values for "category" in principals we could load.
Probably the most common is "self" when a person appeared in a production
without portraying some other character.  Talk shows, documentaries, and
cameos in TV or movie are all such "self" appearances.  There are also
cinematographers, producers, composers, etc.

There are title.akas (alternate and foreign titles)
*/
