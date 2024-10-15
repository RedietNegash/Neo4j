// Let's load data from our CSV and create User, Movie, Genre, and Cast nodes
LOAD CSV WITH HEADERS FROM 'file:///df_movies_data.csv' AS row
WITH row, 
     split(row.genres, '|') AS genres_list, 
     split(row.cast, '|') AS cast_list  // We will split the cast field into a list

// Now, we will create User and Movie nodes if they don't exist
MERGE (u:User {id: row.userId})
MERGE (m:Movie {id: row.movieId})
SET m.title = row.title,
    m.popularity = toFloat(row.popularity)  // Let's set the movie title and popularity

// We will create Rating relationships between User and Movie
MERGE (u)-[r:RATED {rating: row.rating}]->(m)

// Next, let's create Genre nodes and link them to Movies
FOREACH (genre IN genres_list | 
    MERGE (g:Genre {name: genre}) 
    MERGE (m)-[:IN_GENRE]->(g)
)

// We will also create Cast nodes and link them to Movies
FOREACH (cast IN cast_list | 
    MERGE (a:Cast {name: cast})
    MERGE (a)-[:ACTED_IN]->(m)
);

// Now, let’s verify if the nodes were created successfully
MATCH (user:User)
RETURN user
LIMIT 15

MATCH (movie:Movie)
RETURN movie
LIMIT 10

MATCH (genre:Genre)
RETURN genre
LIMIT 15

MATCH (actor:Cast)
RETURN actor
LIMIT 15;


// The 1st recommendation is based on the movies rated by the user and the actors involved in those movies.

// First We will find all movies rated by user 15 along with the actors involved.
MATCH (user:User {id: '15'})-[rated:RATED]->(movie:Movie)<-[:ACTED_IN]-(actor:Cast)
RETURN movie.title AS MovieTitle, actor.name AS CastName
LIMIT 15;


// Let’s visualize the results to see what we've got
MATCH (user:User {id: '15'})-[rated:RATED]->(movie:Movie)<-[:ACTED_IN]-(actor:Cast)
RETURN user, rated, movie, actor
LIMIT 15;

// Now, we will find all the movies the user hasn’t rated but the actors are involved in
MATCH (user:User {id: '15'})-[r:RATED]->(movie:Movie)<-[:ACTED_IN]-(actor:Cast)
MATCH (actor)-[:ACTED_IN]->(recMovies:Movie)
WHERE NOT (user)-[:RATED]->(recMovies)
RETURN DISTINCT recMovies.title AS recMovieTitle, actor.name AS ActorName
LIMIT 15;

// This is part of our first recommendation
// retrun only recommended movie titles
MATCH (user:User {id: '15'})-[r:RATED]->(movie:Movie)<-[:ACTED_IN]-(actor:Cast)
MATCH (actor)-[:ACTED_IN]->(recMovies:Movie)
WHERE NOT (user)-[:RATED]->(recMovies)
RETURN DISTINCT recMovies.title AS recMovieTitle
LIMIT 15;



// The 2nd recommendation is based on the genres of movies rated by the user.
// Now, let’s get the movies the user has rated along with their genres


MATCH (user:User {id: '15'})-[r:RATED]->(movie:Movie)-[:IN_GENRE]->(genre:Genre)
RETURN movie.title AS MovieTitle, collect(genre.name) AS Genres
LIMIT 15;


// We’ll visualize the user's rated movies and their genres
MATCH (user:User {id: '15'})-[r:RATED]->(movie:Movie)-[:IN_GENRE]->(genre:Genre)
RETURN user, movie, genre
LIMIT 15


// We will find similar movies that share genres the user hasn’t rated yet.
MATCH (user:User {id: '15'})-[r:RATED]->(ratedMovie:Movie)-[:IN_GENRE]->(genre:Genre)
MATCH (genre)<-[:IN_GENRE]-(similarMovies:Movie)
WHERE NOT (user)-[:RATED]->(similarMovies)
RETURN DISTINCT similarMovies.title AS SimilarMovieTitle
LIMIT 15;


// Finally, let’s visualize the user, rated movies, genres and similar movies
MATCH (user:User {id: '15'})-[r:RATED]->(ratedMovie:Movie)-[:IN_GENRE]->(genre:Genre)
MATCH (genre)<-[:IN_GENRE]-(similarMovies:Movie)
WHERE NOT (user)-[:RATED]->(similarMovies)
RETURN user, ratedMovie, genre, similarMovies
LIMIT 15;

