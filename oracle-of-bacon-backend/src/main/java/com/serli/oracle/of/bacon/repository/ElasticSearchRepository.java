package com.serli.oracle.of.bacon.repository;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ElasticSearchRepository {

    private final RestHighLevelClient client;

    public ElasticSearchRepository() {
        client = createClient();

    }

    public static RestHighLevelClient createClient() {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")
                )
        );
    }

    public List<String> getActorsSuggests(String searchQuery) throws IOException {
        // TODO implement suggest
    	List<String> suggestions = new ArrayList<String>();     

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        
        SuggestionBuilder<?> completionSuggestionBuilder =
            SuggestBuilders.completionSuggestion("suggestion").prefix(searchQuery, Fuzziness.AUTO).size(5).skipDuplicates(true); 
        
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("suggest_actor", completionSuggestionBuilder); 
        searchSourceBuilder.suggest(suggestBuilder);
        
        SearchRequest searchRequest = new SearchRequest();        
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest); 
        
        Suggest suggest = searchResponse.getSuggest(); 
        CompletionSuggestion completionSuggestion = suggest.getSuggestion("suggest_actor"); 
        
        for (CompletionSuggestion.Entry entry : completionSuggestion.getEntries()) { 
            for (CompletionSuggestion.Entry.Option option : entry) { 
                suggestions.add(option.getHit().getSourceAsMap().get("name").toString());
            }
        }
        return suggestions;
    }
}
