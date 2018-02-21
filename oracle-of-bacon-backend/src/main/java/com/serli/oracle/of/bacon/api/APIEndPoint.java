package com.serli.oracle.of.bacon.api;

import com.google.gson.Gson;
import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;
import com.serli.oracle.of.bacon.repository.MongoDbRepository;
import com.serli.oracle.of.bacon.repository.Neo4JRepository;
import com.serli.oracle.of.bacon.repository.RedisRepository;
import net.codestory.http.annotations.Get;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;

public class APIEndPoint {
    private final Neo4JRepository neo4JRepository;
    private final ElasticSearchRepository elasticSearchRepository;
    private final RedisRepository redisRepository;
    private final MongoDbRepository mongoDbRepository;

    public APIEndPoint() {
        neo4JRepository = new Neo4JRepository();
        elasticSearchRepository = new ElasticSearchRepository();
        redisRepository = new RedisRepository();
        mongoDbRepository = new MongoDbRepository();
    }

    @Get("bacon-to?actor=:actorName")
    public String getConnectionsToKevinBacon(String actorName) {
    	// Add the search to Redis 
    	redisRepository.addSearch(actorName);
    	List<?> connections = neo4JRepository.getConnectionsToKevinBacon(actorName);
    	// Convert array in string 
    	String json = new Gson().toJson(connections);
    	return json; 	
    }

    @Get("suggest?q=:searchQuery")
    public List<String> getActorSuggestion(String searchQuery) throws IOException {
    	return Arrays.asList("Niro, Chel",
                "Senanayake, Niro",
                "Niro, Juan Carlos",
                "de la Rua, Niro",
                "Niro, Simão");
    }

    @Get("last-searches")
    public List<String> last10Searches() {
    	 return redisRepository.getLastTenSearches();
    }

    @Get("actor?name=:actorName")
    public String getActorByName(String actorName) {
    	return this.mongoDbRepository.getActorByName(actorName).map(Document::toJson).orElse("");
    }
}
