package com.serli.oracle.of.bacon.loader.elasticsearch;

import com.serli.oracle.of.bacon.repository.ElasticSearchRepository;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class CompletionLoader {
	private static AtomicInteger count = new AtomicInteger(0);

	public static void main(String[] args) throws IOException, InterruptedException {
		RestHighLevelClient client = ElasticSearchRepository.createClient();

		if (args.length != 1) {
			System.err.println("Expecting 1 arguments, actual : " + args.length);
			System.err.println("Usage : completion-loader <actors file path>");
			System.exit(-1);
		}

		String inputFilePath = args[0];
		try (BufferedReader bufferedReader = Files.newBufferedReader(Paths.get(inputFilePath))) {
			bufferedReader
			.lines()
			.forEach(line -> {
				//TODO ElasticSearch insert                       
				try {
					Map<String, Object> map = new HashMap<>();

					line = line.substring(1, line.length() - 1);
					map.put("name", line);  

					List<String> suggestions = new ArrayList<String>();
					String[] split = line.split(", ");
					for(int i =0; i< split.length; i++){
						suggestions.add(split[i]);
						String[] split2 = split[i].split(" ");
						if(split2.length > 1)
						{
							for(int j =0; j< split2.length; j++){
								suggestions.add(split2[j]);
							}
						}
					}
					map.put("suggestion", suggestions);

					PutMappingRequest request = new PutMappingRequest("actors"); 
					request.type("actor"); 

					XContentBuilder builder = XContentFactory.jsonBuilder();
					builder.startObject();
					{
						builder.startObject("properties");
						{
							builder.field("name", "text");
							builder.field("suggestion", "completion");
						}
						builder.endObject();
					}
					builder.endObject();
					request.source(builder);
					
					//FAUT AJOUTER MAPPING A L'INDEX

					IndexRequest  indexRequest = new IndexRequest("actors", "actor").source(map); 
					client.index(indexRequest);
					count.addAndGet(1);

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}              	
				System.out.println(line);
			});
		}

		System.out.println("Inserted total of " + count.get() + " actors");

		client.close();
	}
}
