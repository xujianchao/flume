package com.ultrapower.flume.extend;

import org.apache.flume.node.Application;

public class FlumeExtend {
	
	public void start(String[] args) {
		Application.main(args);
	}

	public static void main(String[] args) {
		// 发送数据 curl -X POST -d'[{"headers":{"h1":"v1","h2":"v2"},"body":"hello body"}]'  http://10.0.0.225:8083
		final FlumeExtend service = new FlumeExtend();
		String[] ags= {"org.apache.flume.node.Application","-n","hdfs","-f","C:\\Users\\jack\\Documents\\workspace-sts-3.9.5.RELEASE\\flume-extend\\src\\main\\resources\\server_hdfs.conf"};
		service.start(ags);
	}

}
