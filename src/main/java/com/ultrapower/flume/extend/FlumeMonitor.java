package com.ultrapower.flume.extend;

import org.apache.flume.node.Application;

/**
 * flume扩展包
 * @Description flume source sink的扩展包
 * @author chengxj
 * @time 2018年4月26日 下午2:28:10
 */
public class FlumeMonitor {
	
	public void start(String[] args) {
		Application.main(args);
	}

	public static void main(String[] args) {
		final FlumeMonitor service = new FlumeMonitor();
		service.start(args);
	}

}
