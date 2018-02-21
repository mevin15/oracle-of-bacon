package com.serli.oracle.of.bacon.repository;

import redis.clients.jedis.Jedis;

import java.util.List;

public class RedisRepository {
    private final Jedis jedis;

    public RedisRepository() {
        this.jedis = new Jedis("localhost");
    }

    public List<String> getLastTenSearches() {
        return this.jedis.lrange("searches", 0, 9);
    }
    
    public void addSearch(String actorName) {
        this.jedis.lpush("searches", actorName);
    }
}
