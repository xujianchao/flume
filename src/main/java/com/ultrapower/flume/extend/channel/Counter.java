/**  
 * All rights Reserved, Designed By Ultrapower-WH—Develop-Center
 * @Title:  CounterTest.java   
 * @Package com.ultrapower.flume.extend.channel   
 * @Description:    TODO(用一句话描述该文件做什么)   
 * @author: CY     
 * @date:   2018年8月30日 下午6:07:25   
 *  
 * 
 */ 
package com.ultrapower.flume.extend.channel;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**   
 * @ClassName:  CounterTest   
 * @Description:TODO
 * @author: CY
 * @date:   2018年8月30日 下午6:07:25   
 *     
 * @Copyright: ultrapower-WH
 * 
 */
public class Counter {
	
	private  volatile  AtomicInteger count = new AtomicInteger(0);
	
	 
	public Map<String,AtomicInteger> map;
	
    public void clc() {
		
		count.getAndIncrement();
	}
	
	public void clear() {
		count = new AtomicInteger(0);
	}

	public AtomicInteger getCount() {
		return count;
	}
	
	
	
	

}
