/**  
 * All rights Reserved, Designed By Ultrapower-WH—Develop-Center
 * @Title:  HttpClientUtil.java   
 * @Package com.ultrapower.flume.extend.util   
 * @Description:    TODO(用一句话描述该文件做什么)   
 * @author: CY     
 * @date:   2018年8月31日 下午6:02:47   
 *  
 * 
 */ 
package com.ultrapower.flume.extend.util;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**   
 * @ClassName:  HttpClientUtil   
 * @Description:TODO
 * @author: CY
 * @date:   2018年8月31日 下午6:02:47   
 *     
 * @Copyright: ultrapower-WH
 * 
 */
public class HttpClientUtil {
	 private static final Logger log = LoggerFactory.getLogger(HttpClientUtil.class);
	    
	    /**
	     * 初始化HttpClient
	     */
	    private static CloseableHttpClient httpClient = HttpClients.createDefault();
	    
	    
	    
		public static boolean sendHttpRequest(String url ,String message) {
			boolean dto = false;
			HttpClient client = null;
	        try {
	    		client = new DefaultHttpClient();
	            HttpPost httpPost = new HttpPost(url); 
	            StringEntity entity = new StringEntity(message, "UTF-8");  
	            entity.setContentType("text/json");
	            entity.setContentEncoding(new BasicHeader(HTTP.CONTENT_TYPE, "application/json"));
	            entity.setContentEncoding(new BasicHeader(HTTP.DEFAULT_CONTENT_CHARSET, "UTF-8"));
		        httpPost.setEntity(entity);
		        HttpResponse response = client.execute(httpPost);
		        if (response != null && response.getStatusLine().getStatusCode() == 200) {
		        	dto = true;
		        }
			} catch (Exception e) {			
				log.error("下发请求出错:"+url+"---"+ message+"--"+ e.getLocalizedMessage());
			} finally {			
				if (client != null) {
			        client.getConnectionManager().shutdown();
			        client = null;
				}
			}
			return dto;		
		}

	    
	    /**
	     * POST方式调用
	     * 
	     * @param url
	     * @param params 参数为NameValuePair键值对对象
	     * @return 响应字符串
	     * @throws java.io.UnsupportedEncodingException
	     */
	    public static String executeByPOST(String url, List<NameValuePair> params) {
	        HttpPost post = new HttpPost(url);
	        
	        ResponseHandler<String> responseHandler = new BasicResponseHandler();
	        String responseJson = null;
	        try {
	            if (params != null) {
	                post.setEntity(new UrlEncodedFormEntity(params));
	            }
	            responseJson = httpClient.execute(post, responseHandler);
	            log.info("HttpClient POST请求结果：" + responseJson);
	        }
	        catch (ClientProtocolException e) {
	            e.printStackTrace();
	            log.info("HttpClient POST请求异常：" + e.getMessage());
	        }
	        catch (IOException e) {
	            e.printStackTrace();
	        }
	        finally {
	            httpClient.getConnectionManager().closeExpiredConnections();
	            httpClient.getConnectionManager().closeIdleConnections(30, TimeUnit.SECONDS);
	        }
	        return responseJson;
	    }
	    
	    /**
	     * Get方式请求
	     * 
	     * @param url 带参数占位符的URL，例：http://ip/User/user/center.aspx?_action=GetSimpleUserInfo&codes={0}&email={1}
	     * @param params 参数值数组，需要与url中占位符顺序对应
	     * @return 响应字符串
	     * @throws java.io.UnsupportedEncodingException
	     */
	    public static String executeByGET(String url, Object[] params) {
	        String messages = MessageFormat.format(url, params);
	        HttpGet get = new HttpGet(messages);
	        ResponseHandler<String> responseHandler = new BasicResponseHandler();
	        String responseJson = null;
	        try {
	            responseJson = httpClient.execute(get, responseHandler);
	            log.info("HttpClient GET请求结果：" + responseJson);
	        }
	        catch (ClientProtocolException e) {
	            e.printStackTrace();
	            log.error("HttpClient GET请求异常：" + e.getMessage());
	        }
	        catch (IOException e) {
	            e.printStackTrace();
	            log.error("HttpClient GET请求异常：" + e.getMessage());
	        }
	        finally {
	            httpClient.getConnectionManager().closeExpiredConnections();
	            httpClient.getConnectionManager().closeIdleConnections(30, TimeUnit.SECONDS);
	        }
	        return responseJson;
	    }
	    
	    /**
	     * @param url
	     * @return
	     */
	    public static String executeByGET(String url) {
	        HttpGet get = new HttpGet(url);
	        ResponseHandler<String> responseHandler = new BasicResponseHandler();
	        String responseJson = null;
	        try {
	            responseJson = httpClient.execute(get, responseHandler);
	            log.info("HttpClient GET请求结果：" + responseJson);
	        }
	        catch (ClientProtocolException e) {
	            e.printStackTrace();
	            log.error("HttpClient GET请求异常：" + e.getMessage());
	        }
	        catch (IOException e) {
	            e.printStackTrace();
	            log.error("HttpClient GET请求异常：" + e.getMessage());
	        }
	        finally {
	            httpClient.getConnectionManager().closeExpiredConnections();
	            httpClient.getConnectionManager().closeIdleConnections(30, TimeUnit.SECONDS);
	        }
	        return responseJson;
	    }

}
