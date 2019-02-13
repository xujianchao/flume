package com.ultrapower.flume.extend.monitor;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

public class MonitorInfo implements Serializable{
	
	
	private static final long serialVersionUID = 1L;
	private String component;
	private String datatype;
	private String ip;
	private long time;
	private String parameter ;
	private double numvalue;
	private String strvalue;
	private String valuetype;
	private List<String> dataip;
	private List<String> datapath;
	private List<Long> datapathtime;
	private String tag;
	public String getComponent() {
		return component;
	}
	public void setComponent(String component) {
		this.component = component;
	}
	public String getDatatype() {
		return datatype;
	}
	public void setDatatype(String datatype) {
		this.datatype = datatype;
	}
	public String getIp() {
		return ip;
	}
	public void setIp(String ip) {
		this.ip = ip;
	}
	public String getParameter() {
		return parameter;
	}
	public void setParameter(String parameter) {
		this.parameter = parameter;
	}
	public double getNumvalue() {
		return numvalue;
	}
	public void setNumvalue(double numvalue) {
		this.numvalue = numvalue;
	}
	public String getStrvalue() {
		return strvalue;
	}
	public void setStrvalue(String strvalue) {
		this.strvalue = strvalue;
	}
	public String getValuetype() {
		return valuetype;
	}
	public void setValuetype(String valuetype) {
		this.valuetype = valuetype;
	}
	
	public List<String> getDataip() {
		return dataip;
	}
	public void setDataip(List<String> dataip) {
		this.dataip = dataip;
	}
	public List<String> getDatapath() {
		return datapath;
	}
	public void setDatapath(List<String> datapath) {
		this.datapath = datapath;
	}
	public String getTag() {
		return tag;
	}
	public void setTag(String tag) {
		this.tag = tag;
	}
	public List<Long> getDatapathtime() {
		return datapathtime;
	}
	public void setDatapathtime(List<Long> datapathtime) {
		this.datapathtime = datapathtime;
	}
	public Long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
	
	

}
