package com.ultrapower.flume.extend.source.kafka;

import static com.ultrapower.flume.extend.source.kafka.KafkaConfigurationConstants.CONFIG_BROKER;
import static com.ultrapower.flume.extend.source.kafka.KafkaConfigurationConstants.CONFIG_GROUP;
import static com.ultrapower.flume.extend.source.kafka.KafkaConfigurationConstants.CONFIG_TOPIC;
import static com.ultrapower.flume.extend.source.kafka.KafkaConfigurationConstants.DEFAULT_KEY_DESERIALIZER;
import static com.ultrapower.flume.extend.source.kafka.KafkaConfigurationConstants.DEFAULT_VALUE_DESERIALIZER;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSource extends AbstractSource implements EventDrivenSource, Configurable {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
	private final static int AUTO_COMMIT_INTERVAL_MS_CONFIG = 1000;
	private final static int SESSION_TIMEOUT_MS_CONFIG = 30000;
	private volatile String broker;
	private volatile String topic;
	private volatile String group;
	private KafkaConsumer<String, String> consumer;
	private SourceCounter sourceCounter;
	private boolean flag = false;

	public void configure(Context context) {
		broker = context.getString(CONFIG_BROKER);
		topic = context.getString(CONFIG_TOPIC);
		group = context.getString(CONFIG_GROUP);
	    if (sourceCounter == null) {
	        sourceCounter = new SourceCounter(getName());
	    }
	}

	public synchronized void start() {
		Properties conf = new Properties();
		conf.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		conf.put(ConsumerConfig.GROUP_ID_CONFIG, group);
		conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		conf.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		conf.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, AUTO_COMMIT_INTERVAL_MS_CONFIG);
		conf.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, SESSION_TIMEOUT_MS_CONFIG);
		conf.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, DEFAULT_KEY_DESERIALIZER);
		conf.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_DESERIALIZER);
//		conf.put("max.poll.records", 10);
//		conf.put("fetch.min.bytes", "1");
//		conf.put("fetch.wait.max.ms", "1000");
//		conf.put("request.timeout.ms", "40000");
		consumer = new KafkaConsumer<String, String>(conf);
		flag = false;
		sourceCounter.start();
		super.start();
		while (true) {
			if (flag) {
				LOG.info("关闭线程");
				break;
			}
//			TopicPartition partition = new TopicPartition(topic, partitionID);
//			consumer.assign(Arrays.asList(partition));
			consumer.subscribe(Arrays.asList(topic));
			ConsumerRecords<String, String> records = consumer.poll(100);
			List<Event> events = new ArrayList<Event>();
			for (ConsumerRecord<String, String> record : records) {
				String val = record.value();
				Event event = new SimpleEvent();
				event.setBody(val.getBytes());
				events.add(event);
			}
			if (events != null && events.size() > 0) {
				sourceCounter.incrementAppendBatchReceivedCount();
				sourceCounter.addToEventReceivedCount(events.size());
				try {
					getChannelProcessor().processEventBatch(events);
				} catch (Exception e) {
					LOG.error("KafkaSource批量处理消息出错:" + e.getLocalizedMessage());
					return;
				}
				sourceCounter.incrementAppendBatchAcceptedCount();
				sourceCounter.addToEventAcceptedCount(events.size());				
			}
		}
	}

	public synchronized void stop() {
		if (consumer != null) {
			consumer.close();
			consumer = null;
			flag = true;
		}
		sourceCounter.stop();
	}

}
