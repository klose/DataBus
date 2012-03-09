package com.longyi.databus.clientapi;

import java.util.ArrayList;
import java.util.List;

import com.longyi.databus.daemon.DataMapForJob;
import com.longyi.databus.daemon.JobDataMap;
import com.longyi.databus.daemon.PartionUpdateThread;
import com.longyi.databus.define.ValueObject;

public class TaskInput {
	private JobDataMap jobDataMap=null;
	public TaskInput(String jobId,boolean ValueType)
	{
		jobDataMap=DataMapForJob.JobDataMapFactory(jobId,ValueType);
	}
	public List<ValueObject> getkeyObject(String partId,String key)
	{
		return jobDataMap.getkeyObject(partId,key);
	}
	public List<byte[]> getkeyByte(String partId,String key)
	{
		return jobDataMap.getkeyByte(partId,key);
	}
	public ArrayList<String> getkeyList(String partId)
	{
		return jobDataMap.getkeyList(partId);
	}
	public PartionUpdateThread update(String partId)
	{
		return jobDataMap.update(partId);
	}
}
