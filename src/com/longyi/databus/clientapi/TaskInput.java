package com.longyi.databus.clientapi;

import java.util.ArrayList;
import java.util.List;

import com.longyi.databus.daemon.DataMapForJob;
import com.longyi.databus.daemon.JobDataMap;
import com.longyi.databus.daemon.PartionUpdateThread;

public class TaskInput {
	private JobDataMap jobDataMap=null;
	public TaskInput(String jobId,boolean ValueType)
	{
		jobDataMap=DataMapForJob.JobDataMapFactory(jobId,ValueType,null);
	}
	public List<Object> getkeyObject(String partId,String key)
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
	public String[] getPartionLocation(String partId)
	{
		return jobDataMap.getPartionLocation(partId);
	}
	public PartionUpdateThread update(String partId)
	{
		return jobDataMap.update(partId);
	}
}
