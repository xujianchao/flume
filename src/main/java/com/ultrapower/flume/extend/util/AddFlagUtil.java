package com.ultrapower.flume.extend.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

public class AddFlagUtil {

	private static final Logger logger = Logger.getLogger(AddFlagUtil.class);
	
	private static String osName = System.getProperty("os.name");


	/**
	 * add flag on msg
	 * 
	 * @param event
	 */
	public static String addFlag(String event, int port, String path,String ip) {
		JSONObject json = null;
		try {
			json = JSONObject.parseObject(event);

			JSONObject o = json.getJSONObject("default");
			JSONArray dataIp = o.getJSONArray("dataip");
			JSONArray dataPathTime = o.getJSONArray("datapathtime");
			JSONArray datapath = o.getJSONArray("datapath");
//			InetAddress addr = InetAddress.getLocalHost();
//			String ip = addr.getHostAddress().toString(); // 获取本机ip
			if (dataIp != null) {
				dataIp.add(ip);
			}
			if (dataPathTime != null) {
				dataPathTime.add(System.currentTimeMillis());
			}
			if (datapath != null) {
				datapath.add(path + port);
			}
           
			o.put("dataip", dataIp);
			o.put("datapathtime", dataPathTime);
			o.put("datapath", datapath);
			json.put("default", o);

		} catch (Exception e) {
			logger.error("给日志文件打标签出错：" + e.getMessage());
		}
		return JSONObject.toJSONString(json);

	}
	
	public static String getHostIp() {
		String ip ="";
		try {
		if (osName.toLowerCase().contains("windows") || osName.toLowerCase().contains("win")) {
				InetAddress addr = InetAddress.getLocalHost();
				ip = addr.getHostAddress().toString(); // 获取本机ip
		}else {
			String cmd = "hostname -i";
			// String cmd = "wmic process list";
			Runtime rt = Runtime.getRuntime();
			Process ps = rt.exec(cmd);
			InputStream is = ps.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(is));
			String line;
			while ((line = reader.readLine()) != null) {
				ip =line;
				break;
			}
		}
		} catch (IOException e) {
			  try {
				InetAddress addr = InetAddress.getLocalHost();
				ip = addr.getHostAddress().toString(); // 获取本机ip
			} catch (UnknownHostException e1) {
				ip="unkown";
				logger.error("无法获取当前主机ip");
			}
		}
		
		if ( "".equals(ip)) {
			InetAddress addr;
			try {
				addr = InetAddress.getLocalHost();
				ip = addr.getHostAddress().toString(); // 获取本机ip
			} catch (UnknownHostException e) {
				ip = "unkonw";
				logger.error("请配置ip地址");
			}
		}
		
		return ip;
	}

}
