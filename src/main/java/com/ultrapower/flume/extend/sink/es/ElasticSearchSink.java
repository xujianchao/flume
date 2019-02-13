/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.ultrapower.flume.extend.sink.es;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.RestClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.ultrapower.flume.extend.util.AddFlagUtil;
import com.ultrapower.flume.extend.util.CounterUtil;

/**
 * A sink which reads events from a channel and writes them to ElasticSearch
 * based on the work done by https://github.com/Aconex/elasticflume.git.
 * </p>
 * 
 * This sink supports batch reading of events from the channel and writing them
 * to ElasticSearch.
 * </p>
 * 
 * Indexes will be rolled daily using the format 'indexname-YYYY-MM-dd' to allow
 * easier management of the index
 * </p>
 * 
 * This sink must be configured with with mandatory parameters detailed in
 * {@link ElasticSearchSinkConstants}
 * </p>
 * It is recommended as a secondary step the ElasticSearch indexes are optimized
 * for the specified serializer. This is not handled by the sink but is
 * typically done by deploying a config template alongside the ElasticSearch
 * deploy
 * </p>
 * 
 * @see http
 *      ://www.elasticsearch.org/guide/reference/api/admin-indices-templates.
 *      html
 */
public class ElasticSearchSink extends AbstractSink implements Configurable {

	private final static Logger LOGGER = LoggerFactory.getLogger(ElasticSearchSink.class);

	private static final int defaultBatchSize = 2;
	private int batchSize = defaultBatchSize;
	private String clusterName = "";
	private String indexName = "";
	private String indexType = "";
	private RestClient restClient;
	private String ipAndPort="";
	private List<String> bulkData = new ArrayList<String>();
	private String local_ip;
	private String url ="";
	CounterUtil utls =null;

	@Override
	public Status process() throws EventDeliveryException {
		
		Channel channel =null;
		Transaction txn = null;
		Event event = null;
		try {
			channel = getChannel();
			txn = channel.getTransaction();
			txn.begin();
			event = channel.take();
			if(event==null) {
				txn.rollback();
				return Status.BACKOFF;
			}else {
				//给msg添加标记
//				Event e = DeserializeUtil.getInstance().deserializeValue(event.getBody(), true);
				JSONObject json = JSONObject.parseObject(new String(event.getBody()));
				String tag = json.getString("tag");
				if(tag!=null&&"monitor".equals(tag)) {
						json = addFlag(json);
						bulkData.add(json.toJSONString());
						if(utls.on_off){//flume-es流量监控统计
							utls.counter.clc();
						}
				}
			}
		
			if(bulkData.size()==batchSize) {
		    	restBulkInsertData(bulkData);
		    	bulkData.clear();
		    	
			} 
			txn.commit();
			return Status.READY;
		} catch (Throwable ex) {
			txn.commit();
			LOGGER.error("消息发送出错:" + ex.getMessage());
			LOGGER.error("无法消费的消息："+new String(event.getBody()));
			
			return Status.READY;
		} finally {
			if (txn != null) {
				txn.close();
			}
		}
		
	}

	@Override
	public void configure(Context context) {

		if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConfiguration.INDEX_NAME))) {
			this.indexName = context.getString(ElasticSearchSinkConfiguration.INDEX_NAME);
		}

		if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConfiguration.INDEX_TYPE))) {
			this.indexType = context.getString(ElasticSearchSinkConfiguration.INDEX_TYPE);
		}

		if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConfiguration.CLUSTER_NAME))) {
			this.clusterName = context.getString(ElasticSearchSinkConfiguration.CLUSTER_NAME);
		}
		if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConfiguration.IP_PORT))) {
			this.ipAndPort = context.getString(ElasticSearchSinkConfiguration.IP_PORT);
		}

		if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConfiguration.BATCH_SIZE))) {
			this.batchSize = Integer.parseInt(context.getString(ElasticSearchSinkConfiguration.BATCH_SIZE));
		}
		
		if (StringUtils.isNotBlank(context.getString(ElasticSearchSinkConfiguration.SEND_URL))) {
			this.url = context.getString(ElasticSearchSinkConfiguration.SEND_URL);
		}

		Preconditions.checkState(StringUtils.isNotBlank(indexName),
				"Missing Param:" + ElasticSearchSinkConfiguration.INDEX_NAME);
		Preconditions.checkState(StringUtils.isNotBlank(indexType),
				"Missing Param:" + ElasticSearchSinkConfiguration.INDEX_TYPE);
//		Preconditions.checkState(StringUtils.isNotBlank(clusterName),
//				"Missing Param:" + ElasticSearchSinkConfiguration.CLUSTER_NAME);
		Preconditions.checkState(StringUtils.isNotBlank(ipAndPort),
				"Missing Param:" + ElasticSearchSinkConfiguration.IP_PORT);
		Preconditions.checkState(batchSize >= 1, ElasticSearchSinkConfiguration.BATCH_SIZE + " must be greater than 0");
		
		  local_ip = AddFlagUtil.getHostIp();
		    if(local_ip==null||"".equals(local_ip)) {
		    	InetAddress addr;
				try {
					addr = InetAddress.getLocalHost();
					local_ip=addr.getHostAddress().toString(); //获取本机ip 
				} catch (UnknownHostException e) {
					local_ip = "unkonw";
					LOGGER.error("请配置ip地址");
				}  
		    }
	}

	@Override
	public void start() {
		String[] hostAndPorts = ipAndPort.split(",");
		 HttpHost [] host = new HttpHost[hostAndPorts.length];
		for(int i=0;i<hostAndPorts.length;i++) {
			
	          String[] hp = hostAndPorts[i].split(":");
//		      restClient = RestClient
//				.builder(new HttpHost(hp[0], Integer.valueOf(hp[1]), "http")).build();
		      HttpHost tempHost = new HttpHost(hp[0], Integer.valueOf(hp[1]), "http");
		      host[i] = tempHost;
		}
		restClient =RestClient.builder(host).build();
		LOGGER.info("start flume-es sink!");
		//计数统计初始化
		if(null != url){
			CounterUtil.httpUrl=url;
			utls = new CounterUtil("flume-es");
			utls.initFlumeCounter();
		}

	}

	@Override
	public void stop() {
		LOGGER.info("ElasticSearch sink {} stopping");
		if (restClient != null) {
			try {
				restClient.close();
			} catch (IOException e) {
				LOGGER.error("close elasticsearch restcliet error , detail is " + e.getLocalizedMessage());
			}
		}

		super.stop();
	}

	private boolean restBulkInsertData(List<String> bulkData) {
		
	
		String actionMetaData = String.format("{ \"index\" : { \"_index\" : \"%s\", \"_type\" : \"%s\" } }%n",
				this.indexName, "_doc");
		// a list of your documents in JSON strings
		StringBuilder bulkRequestBody = new StringBuilder();
		for (String bulkItem : bulkData) {
			bulkRequestBody.append(actionMetaData);
			bulkRequestBody.append(bulkItem);
			bulkRequestBody.append("\n");
		}
		HttpEntity entity = new NStringEntity(bulkRequestBody.toString(), ContentType.APPLICATION_JSON);
		try {
//			org.elasticsearch.client.Response response = restClient.performRequest("PUT",
//					"/"+this.indexName+"/"+this.indexType+"/_bulk", Collections.emptyMap(), entity);
			org.elasticsearch.client.Response response = restClient.performRequest("POST",
					"/_bulk", Collections.emptyMap(), entity);
			return response.getStatusLine().getStatusCode() == HttpStatus.SC_OK;
		} catch (Exception e) {
		   System.out.println(e.getMessage() );
		}
		return false;
	}
	

	/**
	 * add flag on msg
	 * 
	 * @param event
	 */
	private JSONObject addFlag(JSONObject json) {
		try {
			
			String tag = json.getString("tag");
			if (tag != null && "monitor".equals(tag)) {
				JSONArray dataIp = json.getJSONArray("dataip");
				JSONArray dataPathTime = json.getJSONArray("datapathtime");
				JSONArray datapath = json.getJSONArray("datapath");
//				InetAddress addr = InetAddress.getLocalHost();  
//			    String ip=addr.getHostAddress().toString(); //获取本机ip  
				if(null!= dataIp && null != dataPathTime && null != datapath){
					dataIp.add(local_ip);
					dataPathTime.add(System.currentTimeMillis());
					datapath.add("flume-server-es");
					
					json.put("dataip", dataIp);
					json.put("datapathTime", dataPathTime);
					json.put("datapath", datapath);
					
				}
				json.put("server_time", System.currentTimeMillis());
				json.put("type", "monitor");
			}
		
		} catch (Exception e) {
			e.printStackTrace();
			LOGGER.error("给日志打标签出错!"+e.getMessage());
			return json;
		}
		return json;
	}
	
	

}
