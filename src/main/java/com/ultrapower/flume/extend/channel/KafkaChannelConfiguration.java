package com.ultrapower.flume.extend.channel;


public class KafkaChannelConfiguration {
	
	public static final String KAFKA_PREFIX = "kafka.";
	public static final String BROKER_LIST_KEY = "metadata.broker.list";
	public static final String REQUIRED_ACKS_KEY = "request.required.acks";
	public static final String BROKER_LIST_FLUME_KEY = "brokerList";
	public static final String TOPIC = "topic";
	public static final String GROUP_ID = "group.id";
	public static final String GROUP_ID_FLUME = "groupId";
	public static final String AUTO_COMMIT_ENABLED = "auto.commit.enable";
	public static final String ZOOKEEPER_CONNECT = "zookeeper.connect";
	public static final String ZOOKEEPER_CONNECT_FLUME_KEY = "zookeeperConnect";
	public static final String DEFAULT_GROUP_ID = "flume";
	public static final String DEFAULT_TOPIC = "flume-channel";
	public static final String TIMEOUT = "timeout";
	public static final String DEFAULT_TIMEOUT = "100";
	public static final String CONSUMER_TIMEOUT = "consumer.timeout.ms";
	public static final String PARSE_AS_FLUME_EVENT = "parseAsFlumeEvent";
	public static final boolean DEFAULT_PARSE_AS_FLUME_EVENT = true;
	
	public static final String PARTITION_HEADER_NAME = "partitionIdHeader";
	public static final String STATIC_PARTITION_CONF = "defaultPartitionId";
	public static final String MIGRATE_ZOOKEEPER_OFFSETS = "migrateZookeeperOffsets";
	public static final boolean DEFAULT_MIGRATE_ZOOKEEPER_OFFSETS = true;
	public static final String POLL_TIMEOUT = KAFKA_PREFIX + "pollTimeout";
	public static final long DEFAULT_POLL_TIMEOUT = 500;
	
	public static final String READ_SMALLEST_OFFSET = "readSmallestOffset";
	public static final boolean DEFAULT_READ_SMALLEST_OFFSET = false;
	public static final String KEY_HEADER = "key";
	public static final String DEFAULT_ACKS = "all";
	public static final String KAFKA_CONSUMER_PREFIX = KAFKA_PREFIX + "consumer.";
	public static final String KAFKA_PRODUCER_PREFIX = KAFKA_PREFIX + "producer.";
	public static final String DEFAULT_AUTO_OFFSET_RESET = "earliest";
	
	public static final String DEFAULT_KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
	public static final String DEFAULT_VALUE_SERIAIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
	public static final String DEFAULT_KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
	public static final String DEFAULT_VALUE_DESERIAIZER = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
	//通过API发数据给FLUME_MESSAG的URL地址配置属性
	public static final String SEND_URL ="url";
	public static final String CONFIG_URL_DETAL="/api/monitor/getMonitorConfig";
	public static final String SEND_DATA_URL_DETAIL="/api/monitor/sendMessage";
	public static final String COUNTER_FLAG="on";
}
