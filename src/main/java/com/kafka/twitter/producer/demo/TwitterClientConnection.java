package com.kafka.twitter.producer.demo;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import com.google.common.collect.Lists;
import com.kafka.examples.config.AppConfig;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class TwitterClientConnection {
	private static Hosts hosebirdHosts;
	private static Authentication hosebirdAuth;

	public static Client twitterClient(StatusesFilterEndpoint hosebirdEndpoint, BlockingQueue<String> msgQueue) {
		ClientBuilder builder = new ClientBuilder().name("Hosebird-Client-01").hosts(hosebirdHosts)
				.authentication(hosebirdAuth).endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue));
		Client hosebirdClient = builder.build();

		return hosebirdClient;
	}

	public static StatusesFilterEndpoint getTwitterApiConnection() {
		/**
		 * Declare the host you want to connect to, the endpoint, and authentication
		 * (basic auth or oauth)
		 */
		hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		// Optional: set up track terms
		List<String> terms = Lists.newArrayList("Coronavirus");
		hosebirdEndpoint.trackTerms(terms);

		// These secrets should be read from a config file
		hosebirdAuth = new OAuth1(AppConfig.getAppProperty("twitter.consumerKey"),
				AppConfig.getAppProperty("twitter.consumerSecret"), AppConfig.getAppProperty("twitter.token"),
				AppConfig.getAppProperty("twitter.secret"));
		return hosebirdEndpoint;
	}

}
