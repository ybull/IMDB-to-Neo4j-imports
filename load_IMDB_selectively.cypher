/* Cypher script for loading IMDB into Neo4j

Created to use with my IS497DA class.
Revised Sept 2021

This ALTERNATE way of bulk importing IMDB data allows several ways of
filtering which "titles" get loaded, based on their vote counts and popularity,
then optionally by the titleType field.

Note that detailed comments are in the similar "load_IMDB_BIG.cypher"
file.  This one is less verbose.  */

// Create the most important schema constraint for indexing and uniqueness:
CREATE CONSTRAINT ON (c:Prod) ASSERT c.tconst IS UNIQUE

/* title.ratings.tsv contains the per-title 'weighted average' and count of votes.
Notice that out of ~8.2M "titles", only ~1.18M are even in the ratings file.
Thus this rating-based filtering already eliminates 86% of the whole database.

The title.ratings.tsv file is formatted like this, but we can use the original
.gz version to save disk space:
tconst  averageRating   numVotes
tt0000001       5.7     1809
tt0000002       6.0     233
*/

:auto USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM 'file:///Downloads/IMDB/title.ratings.tsv.gz'
   AS line FIELDTERMINATOR '\t'
   WITH line                          // LIMIT 200
   WHERE toInteger(line.numVotes) >= 500  // load only most well-known titles
   MERGE (n:Prod {tconst: line.tconst})   // create node if doesn't exist yet
     ON CREATE SET
       n.numVotes = toInteger(line.numVotes),
       n.averageRating = toInteger(toFloat(line.averageRating) * 10)

/* Next, we load the descriptive fields from fixed.title.basics.tsv
   but ONLY for the nodes that ALREADY EXIST from ratings above.

title.basics.tsv file (after quote-correction) is formatted like this:
tconst  titleType       primaryTitle    originalTitle   isAdult startYear       endYear runtimeMinutes  genres
tt0000001       short   Carmencita      Carmencita      0       1894    \N      1       Documentary,Short
*/

:auto USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM 'file:///Downloads/IMDB/fixed.title.basics.tsv'
 AS line FIELDTERMINATOR '\t'
 WITH line                           LIMIT 20
 WHERE line.isAdult = '0'  // skip the adult productions
 // AND 'movie' in split(line.titleType, ',')  // Optional. only loads movies.
 MATCH (n:Prod {tconst: line.tconst})
  WHERE exists(n.tconst) // was this node found?  If not, skip it
  SET
     n.titleType =      split(line.titleType,','),
 	   n.title =          line.primaryTitle,
 	   n.originalTitle =  line.originalTitle,
 	   n.startYear =      toInteger(line.startYear),
 	   n.endYear =        toInteger(line.endYear),
 	   n.runtimeMinutes = toInteger(line.runtimeMinutes),
 	   n.genres =         split(line.genres,',')

/* Next, link tvEpisode nodes to their tvSeries nodes.

NOTE: This will end up ONLY loading Episodes that have a lot of votes.

the title.episode.tsv.gz  (no modifications needed) is formatted like this:
tconst  parentTconst    seasonNumber    episodeNumber
tt0041951       tt0041038       1       9
tt0042816       tt0989125       1       17
tt0042889       tt0989125       \N      \N
tt0043426       tt0040051       3       42    */

:auto USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM 'file:///IMDB/title.episode.tsv.gz'
 AS line FIELDTERMINATOR '\t'
 WITH line                          LIMIT 20
   // search for both titles listed in the row:
 MATCH (t:Prod {tconst: line.tconst}), (series:Prod {tconst: line.parentTconst})
  WHERE exists(t.tconst) AND exists(series.tconst)  // were they found?
 MERGE (t)-[ep:EPISODE_OF]->(series)  // create relationship if doesn't exist yet
  ON CREATE SET
       ep.season = toInteger(line.seasonNumber),
       ep.number = toInteger(line.episodeNumber),
       t:Episode,     // add label :Episode
       series:Series  // add label :Series

/* Still need to add more below to complete the imports of people and roles..
*/