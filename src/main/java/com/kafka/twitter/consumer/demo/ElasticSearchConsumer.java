package com.kafka.twitter.consumer.demo;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class ElasticSearchConsumer {
	private static Properties properties = new Properties();
	private static Logger logger = LoggerFactory.getLogger(TwitterConsumerApi.class);
	private static JsonParser jsonParser = new JsonParser();

	public static void sendRecordsToElasticSearch(KafkaConsumer<String, String> consumer,
			RestHighLevelClient elasticSearchClient) {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
			int recordCount = records.count();
			logger.info("Received records from kafka:" + recordCount);
			BulkRequest bulkRequest = new BulkRequest();
			for (ConsumerRecord<String, String> record : records) {
				try {
					IndexRequest indexRequest = new IndexRequest("twitter", "tweets", getRecordId(record.value()))
							.source(record.value(), XContentType.JSON);
					bulkRequest.add(indexRequest);
				} catch (NullPointerException ex) {
					logger.info("Skipping bad data", record.value());
				}

			}
			if (recordCount > 0) {
				try {
					elasticSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
					logger.info("{} tweets sent to elasticseach", recordCount);
					consumer.commitSync();
					logger.info("offsets committed to kafka");
					Thread.sleep(1000);
				} catch (IOException e) {
					logger.error("Error occured during bulk send to elasticsearch", e);
				} catch (InterruptedException e) {
					logger.error("InterruptedException exception occured", e);
				}
			}

		}
	}

	private static String getRecordId(String record) {
		return jsonParser.parse(record).getAsJsonObject().get("id_str").getAsString();
	}

	public static KafkaConsumer<String, String> createKafkaConsumer() {
		logger.info("Creating kafka consumer");
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "Twitter_application");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
		org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(
				properties);
		consumer.subscribe(Arrays.asList("twitter_new"));
		return consumer;
	}

}
