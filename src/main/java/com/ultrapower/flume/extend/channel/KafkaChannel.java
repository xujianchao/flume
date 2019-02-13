package com.ultrapower.flume.extend.channel;

import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.BROKER_LIST_FLUME_KEY;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.DEFAULT_ACKS;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.DEFAULT_AUTO_OFFSET_RESET;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.DEFAULT_GROUP_ID;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.DEFAULT_KEY_DESERIALIZER;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.DEFAULT_KEY_SERIALIZER;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.DEFAULT_MIGRATE_ZOOKEEPER_OFFSETS;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.DEFAULT_PARSE_AS_FLUME_EVENT;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.DEFAULT_POLL_TIMEOUT;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.DEFAULT_TOPIC;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.DEFAULT_VALUE_DESERIAIZER;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.DEFAULT_VALUE_SERIAIZER;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.GROUP_ID_FLUME;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.KAFKA_CONSUMER_PREFIX;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.KAFKA_PRODUCER_PREFIX;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.KEY_HEADER;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.MIGRATE_ZOOKEEPER_OFFSETS;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.PARSE_AS_FLUME_EVENT;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.PARTITION_HEADER_NAME;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.POLL_TIMEOUT;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.STATIC_PARTITION_CONF;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.TOPIC;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.ZOOKEEPER_CONNECT_FLUME_KEY;
import static com.ultrapower.flume.extend.channel.KafkaChannelConfiguration.SEND_URL;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.kafka.KafkaChannelCounter;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.security.JaasUtils;
import org.apache.zookeeper.server.auth.IPAuthenticationProvider;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.ultrapower.flume.extend.monitor.IpPartiton;
import com.ultrapower.flume.extend.monitor.MonitorInfo;
import com.ultrapower.flume.extend.util.AddFlagUtil;
import com.ultrapower.flume.extend.util.CounterUtil;
import com.ultrapower.flume.extend.util.HttpClientUtil;

import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import scala.Option;
import scala.collection.Seq;

@SuppressWarnings("deprecation")
public class KafkaChannel extends BasicChannelSemantics {

	private final static Logger LOGGER = LoggerFactory.getLogger(KafkaChannel.class);
	private static final int ZK_SESSION_TIMEOUT = 30000;
	private static final int ZK_CONNECTION_TIMEOUT = 30000;
	private final Properties producerProps = new Properties();
	private final Properties consumerProps = new Properties();
	private KafkaProducer<String, byte[]> producer = null;

	private final String channelUUID = UUID.randomUUID().toString();
	private AtomicReference<String> topic = new AtomicReference<String>();
	private boolean parseAsFlumeEvent = DEFAULT_PARSE_AS_FLUME_EVENT;
	private String zookeeperConnect = null;
	private String topicStr = DEFAULT_TOPIC;
	private String groupId = DEFAULT_GROUP_ID;
	private String partitionHeader = null;
	private Integer staticPartitionId;
	private boolean migrateZookeeperOffsets = DEFAULT_MIGRATE_ZOOKEEPER_OFFSETS;
	// used to indicate if a rebalance has occurred during the current transaction
	AtomicBoolean rebalanceFlag = new AtomicBoolean();
	private long pollTimeout = DEFAULT_POLL_TIMEOUT;


	private final Map<String, Integer> topicCountMap = Collections.synchronizedMap(new HashMap<String, Integer>());
	// Track all consumers to close them eventually.

	private final List<ConsumerAndRecords> consumers = Collections
			.synchronizedList(new LinkedList<ConsumerAndRecords>());
	private KafkaChannelCounter counter;
	
	CounterUtil utls =null;
	/*
	 * Each ConsumerConnector commit will commit all partitions owned by it. To
	 * ensure that each partition is only committed when all events are actually
	 * done, we will need to keep a ConsumerConnector per thread. See Neha's answer
	 * here: http://grokbase.com/t/kafka/users/13b4gmk2jk/commit-offset-per-topic
	 * Since only one consumer connector will a partition at any point in time, when
	 * we commit the partition we would have committed all events to the final
	 * destination from that partition.
	 *
	 * If a new partition gets assigned to this connector, my understanding is that
	 * all message from the last partition commit will get replayed which may cause
	 * duplicates -- which is fine as this happens only on partition rebalancing
	 * which is on failure or new nodes coming up, which is rare.
	 */
	private final ThreadLocal<ConsumerAndRecords> consumerAndRecords = new ThreadLocal<ConsumerAndRecords>() {
		@Override
		public ConsumerAndRecords initialValue() {
			return createConsumerAndRecords();
		}
	};

	private String local_ip;

	@Override
	public void start() {
		try {
			LOGGER.info("Starting Kafka Channel: " + getName());
			if (migrateZookeeperOffsets && zookeeperConnect != null && !zookeeperConnect.isEmpty()) {
				migrateOffsets();
			}
			producer = new KafkaProducer<String, byte[]>(producerProps);
			// We always have just one topic being read by one thread
			LOGGER.info("Topic = " + topic.get());
			topicCountMap.put(topic.get(), 1);
			counter.start();
			//初始化流量监控计数器
			if(null != CounterUtil.httpUrl){
				utls = new CounterUtil();
				utls.initKafkaCounter();
			}
			super.start();
		} catch (Exception e) {
			LOGGER.error("Could not start producer");
			throw new FlumeException("Unable to create Kafka Connections. "
					+ "Check whether Kafka Brokers are up and that the " + "Flume agent can connect to it.", e);
		}
	}

	@Override
	public void stop() {
		for (ConsumerAndRecords c : consumers) {
			try {
				decommissionConsumerAndRecords(c);
			} catch (Exception ex) {
				LOGGER.warn("Error while shutting down consumer.", ex);
			}
		}
//		sender_future_.cancel(true);
		producer.close();
		counter.stop();
		super.stop();
		LOGGER.info("Kafka channel {} stopped. Metrics: {}", getName(), counter);
	}

	@Override
	protected BasicTransactionSemantics createTransaction() {
		return new KafkaTransaction();
	}

	@Override
	public void configure(Context ctx) {
		CounterUtil.httpUrl=ctx.getString(SEND_URL);
//		httpUrl = ctx.getString(SEND_URL);
		String topicStr = ctx.getString(TOPIC);
		if (topicStr == null || topicStr.isEmpty()) {
			topicStr = DEFAULT_TOPIC;
			LOGGER.info("Topic was not specified. Using " + topicStr + " as the topic.");
		}
		topic.set(topicStr);
		String groupId = ctx.getString(GROUP_ID_FLUME);
		if (groupId == null || groupId.isEmpty()) {
			groupId = DEFAULT_GROUP_ID;
			LOGGER.info("Group ID was not specified. Using " + groupId + " as the group id.");
		}
		String brokerList = ctx.getString(BROKER_LIST_FLUME_KEY);
		if (brokerList == null || brokerList.isEmpty()) {
			throw new ConfigurationException("Broker List must be specified");
		}
		setProducerProps(ctx, brokerList);
		setConsumerProps(ctx, brokerList, groupId);
		parseAsFlumeEvent = ctx.getBoolean(PARSE_AS_FLUME_EVENT, DEFAULT_PARSE_AS_FLUME_EVENT);
		pollTimeout = ctx.getLong(POLL_TIMEOUT, DEFAULT_POLL_TIMEOUT);
		staticPartitionId = ctx.getInteger(STATIC_PARTITION_CONF);
		partitionHeader = ctx.getString(PARTITION_HEADER_NAME);
		migrateZookeeperOffsets = ctx.getBoolean(MIGRATE_ZOOKEEPER_OFFSETS, DEFAULT_MIGRATE_ZOOKEEPER_OFFSETS);
		zookeeperConnect = ctx.getString(ZOOKEEPER_CONNECT_FLUME_KEY);
		if (counter == null) {
			counter = new KafkaChannelCounter(getName());
		}

		local_ip = AddFlagUtil.getHostIp();
	
	}

	private void setProducerProps(Context ctx, String bootStrapServers) {
		producerProps.clear();
		producerProps.put(ProducerConfig.ACKS_CONFIG, DEFAULT_ACKS);
		producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERIALIZER);
		producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_SERIAIZER);
		// Defaults overridden based on config
		producerProps.putAll(ctx.getSubProperties(KAFKA_PRODUCER_PREFIX));
		producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
	}

	protected Properties getProducerProps() {
		return producerProps;
	}

	private void setConsumerProps(Context ctx, String bootStrapServers, String groupId) {
		consumerProps.clear();
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DEFAULT_KEY_DESERIALIZER);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_DESERIAIZER);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, DEFAULT_AUTO_OFFSET_RESET);
		// Defaults overridden based on config
		consumerProps.putAll(ctx.getSubProperties(KAFKA_CONSUMER_PREFIX));
		// These always take precedence over config
		consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
		consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
	}

	protected Properties getConsumerProps() {
		return consumerProps;
	}

	private synchronized ConsumerAndRecords createConsumerAndRecords() {
		try {
			KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(consumerProps);
			ConsumerAndRecords car = new ConsumerAndRecords(consumer, channelUUID);
			LOGGER.info("Created new consumer to connect to Kafka");
			car.consumer.subscribe(Arrays.asList(topic.get()), new ChannelRebalanceListener(rebalanceFlag));
			car.offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
			consumers.add(car);
			return car;
		} catch (Exception e) {
			throw new FlumeException("Unable to connect to Kafka", e);
		}
	}

	private void migrateOffsets() {
		ZkUtils zkUtils = ZkUtils.apply(zookeeperConnect, ZK_SESSION_TIMEOUT, ZK_CONNECTION_TIMEOUT,
				JaasUtils.isZkSecurityEnabled());
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps);
		try {
			Map<TopicPartition, OffsetAndMetadata> kafkaOffsets = getKafkaOffsets(consumer);
			if (!kafkaOffsets.isEmpty()) {
				LOGGER.info("Found Kafka offsets for topic {}. Will not migrate from zookeeper", topicStr);
				LOGGER.debug("Offsets found: {}", kafkaOffsets);
				return;
			}

			LOGGER.info("No Kafka offsets found. Migrating zookeeper offsets");
			Map<TopicPartition, OffsetAndMetadata> zookeeperOffsets = getZookeeperOffsets(zkUtils);
			if (zookeeperOffsets.isEmpty()) {
				LOGGER.warn("No offsets to migrate found in Zookeeper");
				return;
			}

			LOGGER.info("Committing Zookeeper offsets to Kafka");
			LOGGER.debug("Offsets to commit: {}", zookeeperOffsets);
			consumer.commitSync(zookeeperOffsets);
			// Read the offsets to verify they were committed
			Map<TopicPartition, OffsetAndMetadata> newKafkaOffsets = getKafkaOffsets(consumer);
			LOGGER.debug("Offsets committed: {}", newKafkaOffsets);
			if (!newKafkaOffsets.keySet().containsAll(zookeeperOffsets.keySet())) {
				throw new FlumeException("Offsets could not be committed");
			}
		} finally {
			zkUtils.close();
			consumer.close();
		}
	}

	private Map<TopicPartition, OffsetAndMetadata> getKafkaOffsets(KafkaConsumer<String, byte[]> client) {
		Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
		List<PartitionInfo> partitions = client.partitionsFor(topicStr);
		for (PartitionInfo partition : partitions) {
			TopicPartition key = new TopicPartition(topicStr, partition.partition());
			OffsetAndMetadata offsetAndMetadata = client.committed(key);
			if (offsetAndMetadata != null) {
				offsets.put(key, offsetAndMetadata);
			}
		}
		return offsets;
	}

	public static java.util.List<String> convert(Seq<String> seq) {
		return scala.collection.JavaConversions.seqAsJavaList(seq);
	}

	private Map<TopicPartition, OffsetAndMetadata> getZookeeperOffsets(ZkUtils client) {
		Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
		ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupId, topicStr);
		//拿到所有的partition
		List<String> partitions = convert(client.getChildrenParentMayNotExist(topicDirs.consumerOffsetDir()).toSeq());
		for (String partition : partitions) {
			TopicPartition key = new TopicPartition(topicStr, Integer.valueOf(partition));
			Option<String> data = client.readDataMaybeNull(topicDirs.consumerOffsetDir() + "/" + partition)._1();
			if (data.isDefined()) {
				Long offset = Long.valueOf(data.get());
				offsets.put(key, new OffsetAndMetadata(offset));
			}
		}
		return offsets;
	}

	private void decommissionConsumerAndRecords(ConsumerAndRecords c) {
		c.consumer.wakeup();
		c.consumer.close();
	}

	// Force a consumer to be initialized. There are many duplicates in
	// tests due to rebalancing - making testing tricky. In production,
	// this is less of an issue as
	// rebalancing would happen only on startup.
	@VisibleForTesting
	void registerThread() {
		try {
			consumerAndRecords.get();
		} catch (Exception e) {
			LOGGER.error(e.getMessage());
			e.printStackTrace();
		}
	}

	private enum TransactionType {
		PUT, TAKE, NONE
	}

	private class KafkaTransaction extends BasicTransactionSemantics {

		private TransactionType type = TransactionType.NONE;
		private Optional<ByteArrayOutputStream> tempOutStream = Optional.absent();
		// For put transactions, serialize the events and hold them until the commit
		// goes is requested.
		private Optional<LinkedList<ProducerRecord<String, byte[]>>> producerRecords = Optional.absent();
		// For take transactions, deserialize and hold them till commit goes through
		private Optional<LinkedList<Event>> events = Optional.absent();
		private Optional<SpecificDatumWriter<AvroFlumeEvent>> writer = Optional.absent();
		private Optional<SpecificDatumReader<AvroFlumeEvent>> reader = Optional.absent();
		private Optional<LinkedList<Future<RecordMetadata>>> kafkaFutures = Optional.absent();
		private final String batchUUID = UUID.randomUUID().toString();
		// Fine to use null for initial value, Avro will create new ones if this
		// is null
		private BinaryEncoder encoder = null;
		private BinaryDecoder decoder = null;
		private boolean eventTaken = false;

		@Override
		protected void doBegin() throws InterruptedException {
			rebalanceFlag.set(false);
		}

		@Override
		protected void doPut(Event event) throws InterruptedException {
			event = addFlag(event);
			System.out.println(new String (event.getBody()));
			type = TransactionType.PUT;
			if (!producerRecords.isPresent()) {
				producerRecords = Optional.of(new LinkedList<ProducerRecord<String, byte[]>>());
			}
			String key = event.getHeaders().get(KEY_HEADER);
			Integer partitionId = null;
			if(CounterUtil.on_off){//流量监控按照TOPI随机指定分区
				staticPartitionId=utls.getPartion(topic.get());
			}
			try {
				if (staticPartitionId != null) {
					partitionId = staticPartitionId;
				}
				// Allow a specified header to override a static ID
				if (partitionHeader != null) {
					String headerVal = event.getHeaders().get(partitionHeader);
					if (headerVal != null) {
						partitionId = Integer.parseInt(headerVal);
					}
				}
				if (partitionId != null) {
					producerRecords.get().add(new ProducerRecord<String, byte[]>(topic.get(), partitionId, key,
							serializeValue(event, parseAsFlumeEvent)));
				} else {
					producerRecords.get().add(new ProducerRecord<String, byte[]>(topic.get(), key,
							serializeValue(event, parseAsFlumeEvent)));
				}
			} catch (NumberFormatException e) {
				throw new ChannelException("Non integer partition id specified", e);
			} catch (Exception e) {
				throw new ChannelException("Error while serializing event", e);
			}
		}

		protected Event doTake() throws InterruptedException {
			LOGGER.trace("Starting event take");
			type = TransactionType.TAKE;
			try {
				if (!(consumerAndRecords.get().uuid.equals(channelUUID))) {
					LOGGER.info("UUID mismatch, creating new consumer");
					decommissionConsumerAndRecords(consumerAndRecords.get());
					consumerAndRecords.remove();
				}
			} catch (Exception ex) {
				LOGGER.warn("Error while shutting down consumer", ex);
			}
			if (!events.isPresent()) {
				events = Optional.of(new LinkedList<Event>());
			}
			Event e;
			// Give the channel a chance to commit if there has been a rebalance
			if (rebalanceFlag.get()) {
				LOGGER.debug("Returning null event after Consumer rebalance.");
				return null;
			}
			if (!consumerAndRecords.get().failedEvents.isEmpty()) {
				e = consumerAndRecords.get().failedEvents.removeFirst();
			} else {
				if (LOGGER.isTraceEnabled()) {
					LOGGER.trace("Assignment during take: {}",
							consumerAndRecords.get().consumer.assignment().toString());
				}
				try {
					long startTime = System.nanoTime();
					if (!consumerAndRecords.get().recordIterator.hasNext()) {
						consumerAndRecords.get().poll();
					}
					if (consumerAndRecords.get().recordIterator.hasNext()) {
						ConsumerRecord<String, byte[]> record = consumerAndRecords.get().recordIterator.next();
						try {
							e = deserializeValue(record.value(), parseAsFlumeEvent);
						} catch (IndexOutOfBoundsException e1) {
							// TODO Auto-generated catch block
							LOGGER.debug(new String(record.value())+"无需转义数据.............."+e1.getMessage());
							e = deserializeValue(record.value(), false);
						}
//						if(on_off){
//							int patition = record.partition();
//							String topic = record.topic();
//							String key = topic+"@partation@"+patition;
//							//计数开始
//							monitorCounter.get(key).clc();
//						}
						// 給日志打标签
						TopicPartition tp = new TopicPartition(record.topic(), record.partition());
						OffsetAndMetadata oam = new OffsetAndMetadata(record.offset() + 1, batchUUID);
						consumerAndRecords.get().saveOffsets(tp, oam);
						
						// Add the key to the header
						if (record.key() != null) {
							e.getHeaders().put(KEY_HEADER, record.key());
						}

						long endTime = System.nanoTime();
						counter.addToKafkaEventGetTimer((endTime - startTime) / (1000 * 1000));

						if (LOGGER.isDebugEnabled()) {
							LOGGER.debug("{} processed output from partition {} offset {}",
									new Object[] { getName(), record.partition(), record.offset() });
						}
					} else {
						return null;
					}
				} catch (Exception ex) {
					LOGGER.warn("Error while getting events from Kafka. This is usually caused by "
							+ "trying to read a non-flume event. Ensure the setting for "
							+ "parseAsFlumeEvent is correct", ex);
					throw new ChannelException("Error while getting events from Kafka", ex);
				}
			}
			eventTaken = true;
			events.get().add(e);
			return e;
		}

      

		/**
		 * add flag on msg
		 * 
		 * @param event
		 */
		public Event addFlag(Event event) {
			try {
				JSONObject json = JSONObject.parseObject(new String(event.getBody()));
				String osVersion = json.getString("osVersion");
				if (osVersion != null) {
					JSONArray dataIp = json.getJSONArray("dataip") == null ? new JSONArray()
							: json.getJSONArray("dataip");
					JSONArray dataPathTime = json.getJSONArray("datapathtime") == null ? new JSONArray()
							: json.getJSONArray("datapathtime");
					JSONArray datapath = json.getJSONArray("datapath") == null ? new JSONArray()
							: json.getJSONArray("datapath");
//					InetAddress addr = InetAddress.getLocalHost();
//					String ip = addr.getHostAddress().toString(); // 获取本机ip
					dataIp.add(local_ip);
					dataPathTime.add(System.currentTimeMillis());
					datapath.add("flume-server");
					json.put("dataip", dataIp);
					json.put("datapathtime", dataPathTime);
					json.put("datapath", datapath);
					event.setBody(json.toJSONString().getBytes());
				}

			} catch (Exception e) {
				LOGGER.error("给日志文件打标签出错：" + e.getMessage());
			}
			return event;

		}


		
		@Override
		protected void doCommit() throws InterruptedException {
			LOGGER.trace("Starting commit");
			if (type.equals(TransactionType.NONE)) {
				return;
			}
			if (type.equals(TransactionType.PUT)) {
				if (!kafkaFutures.isPresent()) {
					kafkaFutures = Optional.of(new LinkedList<Future<RecordMetadata>>());
				}
				try {
					long batchSize = producerRecords.get().size();
					long startTime = System.nanoTime();
					int index = 0;
					for (ProducerRecord<String, byte[]> record : producerRecords.get()) {
						index++;
						if(CounterUtil.on_off){//流量监控计数
							int patition = record.partition();
							String topic = record.topic();
							String key = topic+"@partition@"+patition;
							//计数开始
							CounterUtil.monitorCounter.get(key).clc();
						}
//						Log.info(new String(record.value()));
						kafkaFutures.get().add(producer.send(record, new ChannelCallback(index, startTime)));
						
					}
					// prevents linger.ms from being a problem
					producer.flush();

					for (Future<RecordMetadata> future : kafkaFutures.get()) {
						future.get();
					}
					long endTime = System.nanoTime();
					counter.addToKafkaEventSendTimer((endTime - startTime) / (1000 * 1000));
					counter.addToEventPutSuccessCount(batchSize);
					producerRecords.get().clear();
					kafkaFutures.get().clear();
					
				} catch (Exception ex) {
					LOGGER.warn("Sending events to Kafka failed", ex);
					throw new ChannelException("Commit failed as send to Kafka failed", ex);
				}
			} else {
				// event taken ensures that we have collected events in this transaction
				// before committing
				if (consumerAndRecords.get().failedEvents.isEmpty() && eventTaken) {
					LOGGER.trace("About to commit batch");
					long startTime = System.nanoTime();
					consumerAndRecords.get().commitOffsets();
					long endTime = System.nanoTime();
					counter.addToKafkaCommitTimer((endTime - startTime) / (1000 * 1000));
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug(consumerAndRecords.get().getCommittedOffsetsString());
					}
				}

				int takes = events.get().size();
				if (takes > 0) {
					counter.addToEventTakeSuccessCount(takes);
					events.get().clear();
				}
			}
		}

		@Override
		protected void doRollback() throws InterruptedException {
			if (type.equals(TransactionType.NONE)) {
				return;
			}
			if (type.equals(TransactionType.PUT)) {
				producerRecords.get().clear();
				kafkaFutures.get().clear();
			} else {
				counter.addToRollbackCounter(events.get().size());
				consumerAndRecords.get().failedEvents.addAll(events.get());
				events.get().clear();
			}
		}

		private byte[] serializeValue(Event event, boolean parseAsFlumeEvent) throws IOException {
			byte[] bytes;
			if (parseAsFlumeEvent) {
				if (!tempOutStream.isPresent()) {
					tempOutStream = Optional.of(new ByteArrayOutputStream());
				}
				if (!writer.isPresent()) {
					writer = Optional.of(new SpecificDatumWriter<AvroFlumeEvent>(AvroFlumeEvent.class));
				}
				tempOutStream.get().reset();
				AvroFlumeEvent e = new AvroFlumeEvent(toCharSeqMap(event.getHeaders()),
						ByteBuffer.wrap(event.getBody()));
				encoder = EncoderFactory.get().directBinaryEncoder(tempOutStream.get(), encoder);
				writer.get().write(e, encoder);
				encoder.flush();
				bytes = tempOutStream.get().toByteArray();
			} else {
				bytes = event.getBody();
			}
			return bytes;
		}

		@SuppressWarnings("unchecked")
		private Event deserializeValue(byte[] value, boolean parseAsFlumeEvent) throws IOException {
			Event e;
			if (parseAsFlumeEvent) {
				ByteArrayInputStream in = new ByteArrayInputStream(value);
				decoder = DecoderFactory.get().directBinaryDecoder(in, decoder);
				if (!reader.isPresent()) {
					reader = Optional.of(new SpecificDatumReader<AvroFlumeEvent>(AvroFlumeEvent.class));
				}
				AvroFlumeEvent event = reader.get().read(null, decoder);
				e = EventBuilder.withBody(event.getBody().array(), toStringMap(event.getHeaders()));
			} else {
				e = EventBuilder.withBody(value, Collections.EMPTY_MAP);
			}
			return e;
		}
	}

	/**
	 * Helper function to convert a map of String to a map of CharSequence.
	 */
	private static Map<CharSequence, CharSequence> toCharSeqMap(Map<String, String> stringMap) {
		Map<CharSequence, CharSequence> charSeqMap = new HashMap<CharSequence, CharSequence>();
		for (Map.Entry<String, String> entry : stringMap.entrySet()) {
			charSeqMap.put(entry.getKey(), entry.getValue());
		}
		return charSeqMap;
	}

	/**
	 * Helper function to convert a map of CharSequence to a map of String.
	 */
	private static Map<String, String> toStringMap(Map<CharSequence, CharSequence> charSeqMap) {
		Map<String, String> stringMap = new HashMap<String, String>();
		for (Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
			stringMap.put(entry.getKey().toString(), entry.getValue().toString());
		}
		return stringMap;
	}

	/* Object to store our consumer */
	private class ConsumerAndRecords {

		final KafkaConsumer<String, byte[]> consumer;
		final String uuid;
		final LinkedList<Event> failedEvents = new LinkedList<Event>();

		ConsumerRecords<String, byte[]> records;
		Iterator<ConsumerRecord<String, byte[]>> recordIterator;
		Map<TopicPartition, OffsetAndMetadata> offsets;

		ConsumerAndRecords(KafkaConsumer<String, byte[]> consumer, String uuid) {
			this.consumer = consumer;
			this.uuid = uuid;
			this.records = ConsumerRecords.empty();
			this.recordIterator = records.iterator();
		}

		private void poll() {
			LOGGER.trace("Polling with timeout: {}ms channel-{}", pollTimeout, getName());
			try {
				records = consumer.poll(pollTimeout);
				recordIterator = records.iterator();
				LOGGER.debug("{} returned {} records from last poll", getName(), records.count());
			} catch (WakeupException e) {
				LOGGER.trace("Consumer woken up for channel {}.", getName());
			}
		}

		private void commitOffsets() {
			try {
				consumer.commitSync(offsets);
			} catch (Exception e) {
				LOGGER.info("Error committing offsets.", e);
			} finally {
				LOGGER.trace("About to clear offsets map.");
				offsets.clear();
			}
		}

		private String getOffsetMapString() {
			StringBuilder sb = new StringBuilder();
			sb.append(getName()).append(" current offsets map: ");
			for (TopicPartition tp : offsets.keySet()) {
				sb.append("p").append(tp.partition()).append("-").append(offsets.get(tp).offset()).append(" ");
			}
			return sb.toString();
		}

		// This prints the current committed offsets when debug is enabled
		private String getCommittedOffsetsString() {
			StringBuilder sb = new StringBuilder();
			sb.append(getName()).append(" committed: ");
			for (TopicPartition tp : consumer.assignment()) {
				try {
					sb.append("[").append(tp).append(",").append(consumer.committed(tp).offset()).append("] ");
				} catch (NullPointerException npe) {
					LOGGER.debug("Committed {}", tp);
				}
			}
			return sb.toString();
		}

		private void saveOffsets(TopicPartition tp, OffsetAndMetadata oam) {
			offsets.put(tp, oam);
			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace(getOffsetMapString());
			}
		}
	}

  
	private String getIp(List<IpPartiton> ipps,String partition ) {
		
		for(IpPartiton ipp:ipps) {
			if(partition.equals(ipp.getPartition())) {
				return ipp.getIp();
			}
		}
		return "unknow";
		
	}

}

// Throw exception if there is an error
class ChannelCallback implements Callback {
	private static final Logger log = LoggerFactory.getLogger(ChannelCallback.class);
	private int index;
	private long startTime;

	public ChannelCallback(int index, long startTime) {
		this.index = index;
		this.startTime = startTime;
	}

	public void onCompletion(RecordMetadata metadata, Exception exception) {
		if (exception != null) {
			log.trace("Error sending message to Kafka due to " + exception.getMessage());
		}
		if (log.isDebugEnabled()) {
			long batchElapsedTime = System.currentTimeMillis() - startTime;
			if (metadata != null) {
				log.debug("Acked message_no " + index + ": " + metadata.topic() + "-" + metadata.partition() + "-"
						+ metadata.offset() + "-" + batchElapsedTime);
			}
		}
	}
}

class ChannelRebalanceListener implements ConsumerRebalanceListener {
	private static final Logger log = LoggerFactory.getLogger(ChannelRebalanceListener.class);
	private AtomicBoolean rebalanceFlag;

	public ChannelRebalanceListener(AtomicBoolean rebalanceFlag) {
		this.rebalanceFlag = rebalanceFlag;
	}

	// Set a flag that a rebalance has occurred. Then we can commit the currently
	// written transactions
	// on the next doTake() pass.
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		for (TopicPartition partition : partitions) {
			log.info("topic {} - partition {} revoked.", partition.topic(), partition.partition());
			rebalanceFlag.set(true);
		}
	}

	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		for (TopicPartition partition : partitions) {
			log.info("topic {} - partition {} assigned.", partition.topic(), partition.partition());
		}
	}
	
	
}
