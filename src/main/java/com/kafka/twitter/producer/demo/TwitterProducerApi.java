package com.kafka.twitter.producer.demo;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;

public class TwitterProducerApi {
	private static Logger logger = LoggerFactory.getLogger(TwitterProducerApi.class);
	private static BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

	public static void main(String[] args) {
		StatusesFilterEndpoint hosebirdEndpoint = TwitterClientConnection.getTwitterApiConnection();
		Client hosebirdClient = TwitterClientConnection.twitterClient(hosebirdEndpoint, msgQueue);
		KafkaProducer<String, String> kafkaProducer = TwitterProducer.getNewKafkaProducer();
		hosebirdClient.connect();
		TwitterProducer.sendTweetsToKafka(hosebirdClient, kafkaProducer, msgQueue);
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Received shutdown hook");
			hosebirdClient.stop();
			kafkaProducer.close();
		}));

	}

}
