package com.ultrapower.flume.extend.sink.http;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpSink extends AbstractSink implements Configurable {

	private static final Logger LOG = LoggerFactory.getLogger(HttpSink.class);
	private volatile String master;
	private volatile String slave;
	private volatile Integer port;

	public void configure(Context context) {
		master = context.getString(HttpConfigurationConstants.CONFIG_MASTER);
		slave = context.getString(HttpConfigurationConstants.CONFIG_SLAVE);
		port = context.getInteger(HttpConfigurationConstants.CONFIG_PORT);
	}

	public Status process() throws EventDeliveryException {
		Channel channel = null;
		Transaction transaction = null;
		HttpService service = null;
		if (master != null && !"".equals(master.trim())) {
			service = new HttpService(master, slave);
		} else {
			service = new HttpService(port);
		}
		try {
			channel = getChannel();
			transaction = channel.getTransaction();
			transaction.begin();
			Event event = channel.take();
			if (event == null) {
				transaction.rollback();
				return Status.BACKOFF;
			}
			String body = new String(event.getBody());
			LOG.info("body=" + body);
			service.sendRequest(body);
			transaction.commit();
			return Status.READY;
		} catch (Exception e) {
			LOG.error("消息发送出错:" + e.getLocalizedMessage());
			transaction.rollback();
			return Status.BACKOFF;
		} finally {
			if (transaction != null) {				
				transaction.close();
			}
		}
	}

}
