package com.kafka.examples.config;

import java.util.ResourceBundle;

public class AppConfig {
	private static final ResourceBundle APPPROP = ResourceBundle.getBundle("application");

	public static String getAppProperty(String key) {
		return APPPROP.getString(key);
	}

}
