# IMDB-to-Neo4j-imports
Scripts to selectively load from IMDB's downloadable TSV files 
into a Neo4j graph database.

IMDB makes a large portion of their database downloadable for many 
non-commercial, non-competitive purposes.  See their license for details.

The metadata and files are all reachable here: https://www.imdb.com/interfaces/

Unfortunately, some of the tab-separated values in those files 
use quote delimiters but in ways that aren't compatible with
Neo4j's LOAD CSV command.

So I've included a Python script that can fix those where needed
and also a Cypher script that you can use to load most of the
data in those files in useful ways.

UPDATE Spring 2023: The Cypher language and critical features of the LOAD CSV command 
were so radically changed with Neo4j V.5 that most of this no longer works.
A current workaround is to create the database as version 4.x, load the data
and then upgrade it if desired.
