from neo4j import GraphDatabase


class Neo4jConnection:
    def __init__(self, uri, user, pwd):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
            print("Neo4j connection initialized.")
        except Exception as e:
            print("Failed to create the driver:", e)


    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        try:
            with self.__driver.session(database=db) as session:
                result = session.run(query, parameters)
                return [record.data() for record in result]
        except Exception as e:
            print("Query failed:", e)
            return []


    def get_movie_statistics(self):
        query = """
        MATCH (m:Movie)
        RETURN m.title AS movie, COUNT(*) AS ratingCount
        ORDER BY ratingCount DESC
        """
        return self.query(query)

    def get_recommendations(self, user_id):
        query = """
        MATCH (u:User {userId: $userId})-[:RATED]->(m:Movie)<-[:RATED]-(other:User)-[:RATED]->(rec:Movie)
WHERE NOT EXISTS((u)-[:RATED]->(rec))
RETURN rec.title AS movie, COUNT(*) AS score
ORDER BY score DESC LIMIT 10

        """
        return self.query(query, parameters={"userId": user_id})

    def get_centrality(self):
        query = """
        MATCH (u:User)-[r:RATED]->(m:Movie)
        RETURN u.userId AS user, count(r) AS centrality ORDER BY centrality DESC
        """
        return self.query(query)

    def get_path_analysis(self):
        query = """
        MATCH (u1:User)-[:RATED]->(m:Movie)
        WITH u1, collect(m) AS movies
        LIMIT 1  // Choose the first user found
        UNWIND movies AS movie
        MATCH (movie)<-[:RATED]-(u2:User)
        WHERE u1 <> u2
        RETURN u1.userId AS user1, u2.userId AS user2, collect(movie.title) AS movies
        LIMIT 10;
        """
        return self.query(query)

    def get_community_detection(self):
        query = """
        CALL gds.louvain.stream('userGraph') 
        YIELD nodeId, communityId 
        MATCH (m:Movie) WHERE id(m) = nodeId 
        RETURN communityId, collect(m.title) AS movies 
        LIMIT 5; 
        """
        return self.query(query)
