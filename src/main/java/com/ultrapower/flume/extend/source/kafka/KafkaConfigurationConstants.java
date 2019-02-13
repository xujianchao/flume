package com.ultrapower.flume.extend.source.kafka;

public class KafkaConfigurationConstants {
	
	public static final String CONFIG_BROKER = "broker";
	public static final String CONFIG_TOPIC = "topic";
	public static final String CONFIG_GROUP = "group";
	public static final String CONFIG_PARTITION = "partition";
	public static final String CONFIG_KEY_DESERIALIZER = "key.deserializer";
	public static final String CONFIG_VALUE_DESERIALIZER = "value.deserializer";	
	public static final String DEFAULT_KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String DEFAULT_VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	
}