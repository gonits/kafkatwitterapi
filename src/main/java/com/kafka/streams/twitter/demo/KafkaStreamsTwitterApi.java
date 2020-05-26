package com.kafka.streams.twitter.demo;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonParser;

public class KafkaStreamsTwitterApi {
	private static JsonParser parser = new JsonParser();
	private static Logger logger = LoggerFactory.getLogger(KafkaStreamsTwitterApi.class);

	public static void main(String[] args) {
		Properties properties = new Properties();

		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "KAFKASTREAMS-TWITTER-DEMO");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> inputStream = builder.stream("twitter_new");
		KStream<String, String> filteredStream = inputStream
				.filter((k, jsonTweet) -> extractUserFollowersInTweet(jsonTweet) > 10000);
		logger.info("streams filtered:", filteredStream.toString());
		filteredStream.to("important_tweets");
		KafkaStreams streams = new KafkaStreams(builder.build(), properties);
		streams.start();
	}

	private static int extractUserFollowersInTweet(String jsonTweet) {
		try {
			return parser.parse(jsonTweet).getAsJsonObject().get("user").getAsJsonObject().get("followers_count")
					.getAsInt();
		} catch (NullPointerException ex) {
			logger.info("Skipping records having no followers count");
		}
		return 0;

	}

}
