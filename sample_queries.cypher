// Did these two people act together?  In what?
MATCH p=(start:Person {name:"Tom Hanks"})-[:ACTED_IN*..2]-(end:Person {name:"Meg Ryan"}) RETURN p

// Same query, but only show the Production nodes, not paths:
MATCH p=(start:Person {name:"Tom Hanks"})-[:ACTED_IN]-(t:Prod)-[:ACTED_IN]-(end:Person {name:"Meg Ryan"})
RETURN t



// These two didn't ever act together.  But are they 2-degrees apart?  How?
MATCH p=(start:Person {name:"Tom Hanks"})-[:ACTED_IN*..4]-(end:Person {name:"Harrison Ford"})
RETURN p


// What if we just want a list of actors who are within N degrees of a
// single specific actress?  Or starting from one movie?


// What might we do to the graph data itself if we need to be able to calculate
//  degrees of separation between ANY pairs of people, quickly?



// Determine avg and max number of actors in all productions
MATCH (p:Prod)
RETURN avg(apoc.node.degree(p,'ACTED_IN')) as average_actors,
       max(apoc.node.degree(p,'ACTED_IN')) as max_actors

// How can we use that structure to find the statistics on numbers of EPISODES
// per TV Series?



// Iterate over all node labels for any graph and returns the count of matching nodes
// Notice this example, which I took straight from the documentation is also
//   potentially vulnerable to a code injection!!
call db.labels() yield label
call apoc.cypher.run("match (:`"+label+"`) return count(*) as count", null) yield value
return label, value.count as count

