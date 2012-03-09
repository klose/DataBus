package com.longyi.databus.clientapi;

import java.util.ArrayList;
import java.util.List;

import com.longyi.databus.daemon.DataMapForJob;
import com.longyi.databus.daemon.JobDataMap;
import com.longyi.databus.define.ValueObject;

public class TaskOutput {
	private JobDataMap jobDataMap=null;
	public TaskOutput(String jobId,boolean ValueType)
	{
		jobDataMap=DataMapForJob.JobDataMapFactory(jobId,ValueType);
	}
	public boolean putkeyObject(String partId,String key,List<ValueObject> value)
	{
		return jobDataMap.putkeyObject(partId,key,value);
	}
	public boolean putkeyByte(String partId,String key,List<byte[]> value)
	{
		return jobDataMap.putkeyByte(partId,key,value);
	}
}
