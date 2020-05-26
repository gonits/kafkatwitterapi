package com.kafka.twitter.consumer.demo;

import java.io.IOException;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterConsumerApi {
	private static Logger logger = LoggerFactory.getLogger(TwitterConsumerApi.class);

	public static void main(String[] args) {
		RestHighLevelClient elasticSearchClient = getElasticSearchClient();
		KafkaConsumer<String, String> consumer = ElasticSearchConsumer.createKafkaConsumer();
		ElasticSearchConsumer.sendRecordsToElasticSearch(consumer, elasticSearchClient);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Received shutdown signal"); // need to check why this is not working
			consumer.close();
			try {
				elasticSearchClient.close();
			} catch (IOException e) {
				logger.error("Elasticsearch client could not be closed", e);
			}
		}));

	}

	private static RestHighLevelClient getElasticSearchClient() {
		logger.info("creating elasticsearch client");
		RestClientBuilder builder = RestClient.builder(new HttpHost("127.0.0.1", 9200, "http"));
		return new RestHighLevelClient(builder);
	}

}
