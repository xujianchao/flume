package com.ultrapower.flume.extend.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringEscapeUtils;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ultrapower.flume.extend.channel.Counter;
import com.ultrapower.flume.extend.channel.KafkaChannel;
import com.ultrapower.flume.extend.channel.KafkaChannelConfiguration;
import com.ultrapower.flume.extend.monitor.IpPartiton;

import kafka.security.auth.Topic;
/**
 * 流量监控统计工具类
 * @author xujianchao 20180910
 *
 */
public final class CounterUtil {
	private ScheduledFuture<?> sender_future_ = null;
	private ScheduledExecutorService executor_service_ = null;

	public static final Map<String, Counter> monitorCounter = new HashMap<String, Counter>();
	private final static Logger LOGGER = LoggerFactory.getLogger(CounterUtil.class);
	private JSONObject mappingObj;

	private Map<String, List<IpPartiton>> ipAndPartitions;

	public static boolean on_off = false;//流量监控统计开关
	private static String FLUME_TO_KAFKA = "flumetokafka";

	public static String httpUrl;//流量监控发送接口URL,通过FLUME配置
	Long executeTime = 60l;//流量监控定时器发送默认执行时间s
	public static String local_ip = AddFlagUtil.getHostIp();
	String flumeType=null;
	public Counter counter = null;
	public static String COUNTER_EXECUTE_TIME="kafkaflume";
	public static String COUNTER_EXECUTE_GLOAL_TIME="global";
	public CounterUtil() {
		initConfig();
	}

	public CounterUtil(String flumeType) {
		initConfig();
		this.flumeType = flumeType;
		counter = new Counter();
		
	}
	/**
	 * Es流量监控初始化方法
	 */
	void initConfig(){
		ipAndPartitions = new HashMap<>();
		// 获取配置信息的API URL
		String mappingStr = HttpClientUtil.executeByGET(httpUrl + KafkaChannelConfiguration.CONFIG_URL_DETAL);
		if(null!=mappingStr){
			try {
				mappingStr = StringEscapeUtils.unescapeJava(mappingStr);
				mappingStr = mappingStr.substring(1, mappingStr.length() - 1);
				mappingObj = JSONObject.parseObject(mappingStr);
				JSONObject on_off_json = JSONObject.parseObject(mappingObj.getString("switch"));
				JSONObject timer = JSONObject.parseObject(mappingObj.getString("colFreq"));
				on_off = on_off_json.getString(COUNTER_EXECUTE_GLOAL_TIME).equals(KafkaChannelConfiguration.COUNTER_FLAG) ? true : false;
				if (null != timer.getLong(COUNTER_EXECUTE_TIME)) {
					executeTime = timer.getLong(COUNTER_EXECUTE_TIME);
				} else {
					executeTime = timer.getLong(COUNTER_EXECUTE_GLOAL_TIME);
				} 
			} catch (Exception e) {
				// TODO: handle exception
				LOGGER.error("流量监控获取配置参数失败................."+httpUrl+"------"+mappingStr);
			}
		}else{
			LOGGER.error("流量监控获取配置参数失败................."+httpUrl);
		}
	}

	public void initFlumeCounter() {
		if (on_off) {
			// 启动发送监控数据的线程 executeTime
			executor_service_ = Executors.newScheduledThreadPool(1);
			sender_future_ = executor_service_.scheduleWithFixedDelay(new Runnable() {

				@Override
				public void run() {
					if(counter.getCount().get()>0){
						JSONObject json = new JSONObject();
						json.put("component", flumeType);
						json.put("datatype", "business");
						json.put("ip",local_ip);
						json.put("time", System.currentTimeMillis());
						json.put("parameter", "logcount");
						json.put("valuetype", "number");
						json.put("tag", "monitor");
						json.put("numvalue", counter.getCount().get());
						json.put("strvalue", counter.getCount().get());
						LOGGER.debug("MonitorSenderThread....flume-es counter........................................"+ json.toJSONString());
						HttpClientUtil.sendHttpRequest(httpUrl + KafkaChannelConfiguration.SEND_DATA_URL_DETAIL,
								json.toJSONString());
						counter.clear();
					}
				}
			}, 0L, executeTime, TimeUnit.SECONDS);

		}
	}

	public Map<String, List<IpPartiton>> initKafkaCounter() {
		if (on_off) {
			for (String o : mappingObj.getJSONObject("kafka").keySet()) {
				JSONArray array = JSONObject
						.parseArray(JSONObject.toJSONString(mappingObj.getJSONObject("kafka").get(o)));
				List list = new ArrayList<String>();
				for (int i = 0; i < array.size(); i++) {
					Set<String> keys = array.getJSONObject(i).keySet();
					for (String k : keys) {
						IpPartiton ipn = new IpPartiton();
						ipn.setIp(array.getJSONObject(i).getString(k));
						ipn.setPartition(k);
						list.add(ipn);
						// topic和partition拼接成的key，每个key对应自己的计数器
						String uniqueKey = o + "@" + k.replaceAll("-", "@");
						if (uniqueKey.indexOf("__consumer_offsets") == -1) {
							monitorCounter.put(uniqueKey, new Counter());
						}

					}
					ipAndPartitions.put(o, list);
				}
			}

			// 启动发送监控数据的线程 executeTime
			executor_service_ = Executors.newScheduledThreadPool(1);
			sender_future_ = executor_service_.scheduleWithFixedDelay(new Runnable() {

				@Override
				public void run() {
					int total = 0;
					String topic = "";
					// TODO Auto-generated method stub
					for (Map.Entry<String, Counter> countKey : monitorCounter.entrySet()) {
						Counter counter = countKey.getValue();
						if (counter.getCount().get() > 0) {
							topic = countKey.getKey().split("@")[0];
							String partation = countKey.getKey().split("@")[2];
							JSONObject json = new JSONObject();
							json.put("component", "partition-" + partation);
							json.put("datatype", topic);
							json.put("ip", getIp(ipAndPartitions.get(topic), "partition-" +partation));
							json.put("time", System.currentTimeMillis());
							json.put("parameter", "logcount");
							json.put("valuetype", "number");
							json.put("tag", "monitor");
							json.put("numvalue", counter.getCount().get());
							json.put("strvalue", counter.getCount().get());
							LOGGER.debug(
									"MonitorSenderThread....flumetokafka counter........................................"
											+ json.toJSONString());
							HttpClientUtil.sendHttpRequest(httpUrl + KafkaChannelConfiguration.SEND_DATA_URL_DETAIL,
									json.toJSONString());
							total = total + counter.getCount().get();
							counter.clear();
						}
					}
					if (total > 0) {
						JSONObject json = new JSONObject();
						if (FLUME_TO_KAFKA.equals(topic)) {
							//flume-server流量监控统计
							json.put("component", "flume-server");
						} else {
							//flume-monitor流量监控统计
							json.put("component", "flume-monitor");
						}
						json.put("datatype", "business");
						json.put("ip", local_ip);
						json.put("time", System.currentTimeMillis());
						json.put("parameter", "logcount");
						json.put("valuetype", "number");
						json.put("tag", "monitor");
						json.put("numvalue", total);
						json.put("strvalue", total);
						LOGGER.debug("MonitorSenderThread....flume-server  counter........................................"
								+ json.toJSONString());
						HttpClientUtil.sendHttpRequest(httpUrl + KafkaChannelConfiguration.SEND_DATA_URL_DETAIL,
								json.toJSONString());
					}
				}
			}, 0L, executeTime, TimeUnit.SECONDS);
		}
		return ipAndPartitions;
	}

	private String getIp(List<IpPartiton> ipps, String partition) {

		for (IpPartiton ipp : ipps) {
			if (partition.equals(ipp.getPartition())) {
				return ipp.getIp();
			}
		}
		return "unknow";

	}

	public int getPartion(String topic) {
		int partion = 0;
		JSONArray jarry = JSONArray.parseArray(this.mappingObj.getJSONObject("kafka").get(topic).toString());
		partion = new java.util.Random().nextInt(jarry.size());
		;
		return partion;
	}
}
