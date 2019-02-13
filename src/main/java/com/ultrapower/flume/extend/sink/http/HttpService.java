package com.ultrapower.flume.extend.sink.http;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.ultrapower.flume.extend.common.HttpEntity;

/**
 * 
 * @Description
 * @author chengxj
 * @time 2018年4月27日 上午11:24:47
 */
public class HttpService {
	
	private static Logger log = LoggerFactory.getLogger(HttpService.class);
	private String master;
	private String slave;
	private int port;
	
	public HttpService(int port) {
		this.port = port;
	}

	public HttpService(String master, String slave) {
		this.master = master;
		this.slave = slave;
	}
	
	@SuppressWarnings("unchecked")
	public void sendRequest(String body) throws Exception {
		// slaves 要考虑failover
		if (master != null && !"".equals(master.trim())) {
			try {
				boolean returnVal = sendhttpRequest(master, body);
				if (!returnVal) {
					// 重发
					if (slave != null && !"".equals(slave.trim())) {
						boolean slaveReturnVal = sendhttpRequest(slave, body);
						if (!slaveReturnVal) {
							log.error("httpSink发送消息出错:master=" + master + ":slave=" + slave);
							throw new Exception("httpSink发送消息出错:master=" + master + ":slave=" + slave);
						}					
					}
				}
			} catch (ClientProtocolException e) {
				log.error("httpSink发送消息出错:master=" + master + ":slave=" + slave + ":" + e.getLocalizedMessage());
				throw e;				
			} catch (IOException e) {
				log.error("httpSink发送消息出错:master=" + master + ":slave=" + slave + ":" + e.getLocalizedMessage());
				throw e;
			} catch (Exception e) {
				log.error("httpSink发送消息出错:master=" + master + ":slave=" + slave + ":" + e.getLocalizedMessage());
				throw e;
			}
		} else {
			Map<String, Object> val = JSON.parseObject(body);
			if (val != null && val.containsKey("ip")) {			
				String master = null;
				String slave = null;
				if (val.containsKey("proxy")) {
					// 存在代理
					List<String> proxy = (List<String>)val.get("proxy");
					if (proxy != null && proxy.size() >= 1) {
						String proxyConf = proxy.get(0);
						if (proxyConf != null && !"".equals(proxyConf.trim())) {
							String[] proxys = proxyConf.split(";");
							if (proxys != null && proxys.length >= 1) {
								master = proxys[0];
								log.info("proxy master=" + master);
								if (proxys.length > 1) {
									slave = proxys[1];
									log.info("proxy slave=" + slave);
								}
							}
						}
						proxy.remove(proxyConf);
						if (proxy.size() == 0) {
							val.remove("proxy");
						} else {
							val.put("proxy", proxy);
						}
					} else {
						val.remove("proxy");
					}
				} else {
					// 非代理
					String ip = val.get("ip")==null?null:String.valueOf(val.get("ip"));
					log.info("ip=" + ip + " port=" + port);
					master = ip + ":" + port;
				}
				body = JSON.toJSONString(val);				
				try {
					boolean mReturnVal = sendhttpRequest(master, body);
					if (!mReturnVal) {
						log.error("httpSink发送消息出错:master=" + master);
						if (slave != null && !"".equals(slave.trim())) {
							boolean sReturnVal = sendhttpRequest(slave, body);
							if (!sReturnVal) {
								log.error("httpSink发送消息出错:proxy slave=" + slave);
								throw new Exception("httpSink发送消息出错:proxy slave=" + slave);
							}
						}
					}
				} catch (ClientProtocolException e) {
					log.error("httpSink发送消息出错:master=" +  master + ":slave=" + slave + e.getLocalizedMessage());
					throw e;
				} catch (IOException e) {
					log.error("httpSink发送消息出错:master=" +  master + ":slave=" + slave + e.getLocalizedMessage());
					throw e;
				} catch (Exception e) {
					log.error("httpSink发送消息出错:master=" +  master + ":slave=" + slave + e.getLocalizedMessage());
					throw e;
				}	
			}
		}
	}
	
	private boolean sendhttpRequest(String host, String body) {
		boolean dto = false;
		HttpClient client = null;
        try {
    		client = new DefaultHttpClient();
            HttpPost httpPost = new HttpPost("http://" + host + "/"); 
            HttpEntity metric = new HttpEntity();
            metric.body = body;
            List<HttpEntity>  metrics = new ArrayList<HttpEntity>();
            metrics.add(metric);
            StringEntity entity = new StringEntity(JSON.toJSONString(metrics), "UTF-8");  
            entity.setContentType("text/json");  
            entity.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE, "application/json"));
            entity.setContentEncoding(new BasicHeader(HTTP.DEFAULT_CONTENT_CHARSET, "UTF-8"));
	        httpPost.setEntity(entity);
	        HttpResponse response = client.execute(httpPost);
	        if (response.getStatusLine().getStatusCode() == 200) {
		        dto = true;    	
	        }
        } catch (ClientProtocolException e) {
			log.error("httpSink发送消息出错:master=" +  master + ":slave=" + slave + e.getLocalizedMessage());
			dto = false;
		} catch (IOException e) {
			log.error("httpSink发送消息出错:master=" +  master + ":slave=" + slave + e.getLocalizedMessage());
			dto = false;
		} catch (Exception e) {
			log.error("httpSink发送消息出错:master=" +  master + ":slave=" + slave + e.getLocalizedMessage());
			dto = false;
		} finally {
			if (client != null) {
		        client.getConnectionManager().shutdown();
		        client = null;
			}
		}
        return dto;
	}

}
