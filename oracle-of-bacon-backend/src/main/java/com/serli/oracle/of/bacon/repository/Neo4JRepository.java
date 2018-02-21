package com.serli.oracle.of.bacon.repository;


import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.types.Path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Neo4JRepository {
    private final Driver driver;

    public Neo4JRepository() {
        this.driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "neo4j"));
    }

    public List<?> getConnectionsToKevinBacon(String actorName) {
        Session session = driver.session();
        ArrayList<HashMap<String, GraphItem>> results = new ArrayList<HashMap<String, GraphItem>>();
        
        try (Transaction tx = session.beginTransaction() )
        {
        	StatementResult requestResult = tx.run( "MATCH p=shortestPath( (bacon:Actors {name:\"Bacon, Kevin (I)\"})-[*1..6]-(meg:Actors {name: \""+ actorName +"\"}) ) RETURN bacon, p, meg");
        	
        	if (requestResult.hasNext()) {
        		Path p  = (Path) requestResult.next().asMap().get("p");
        		
        		p.nodes().forEach(node -> {
        			GraphNode gn;
        			String name; 
        			
        			// For Actors node
        			if (node.containsKey("name")) {
        				name = node.get("name").asString();
        				gn = new GraphNode(node.id(), name, "Actor");
        				HashMap<String, GraphItem> hm = new HashMap<String, GraphItem>();
            			hm.put("data", gn);
            			results.add(hm);
        			}
        			
        			// For Movies node 
        			if (node.containsKey("title")){
        				name = node.get("title").asString();
        				gn = new GraphNode(node.id(), name, "Actor");
        				HashMap<String, GraphItem> hm = new HashMap<String, GraphItem>();
            			hm.put("data", gn);
            			results.add(hm);
        			}
        		});
        		
        		p.relationships().forEach(relation -> {
        			GraphEdge ge; 
        			
        			ge = new GraphEdge(relation.id(), relation.startNodeId(), relation.endNodeId(),"PLAYED_IN");
        			HashMap<String, GraphItem> hm = new HashMap<String, GraphItem>();
        			hm.put("data", ge);
        			results.add(hm);
        		});
        		tx.success();	
        	}
            return results;
        }
    }

    public static abstract class GraphItem {
        public final long id;

        private GraphItem(long id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GraphItem graphItem = (GraphItem) o;

            return id == graphItem.id;
        }

        @Override
        public int hashCode() {
            return (int) (id ^ (id >>> 32));
        }
    }

    private static class GraphNode extends GraphItem {
        public final String type;
        public final String value;

        public GraphNode(long id, String value, String type) {
            super(id);
            this.value = value;
            this.type = type;
        }
    }

    private static class GraphEdge extends GraphItem {
        public final long source;
        public final long target;
        public final String value;

        public GraphEdge(long id, long source, long target, String value) {
            super(id);
            this.source = source;
            this.target = target;
            this.value = value;
        }
    }
}
