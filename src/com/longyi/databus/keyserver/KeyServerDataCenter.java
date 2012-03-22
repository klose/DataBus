package com.longyi.databus.keyserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.longyi.databus.define.DATABUS;

public class KeyServerDataCenter {

	public static final Map<String,JobHashMapInfo> JobMap=new ConcurrentHashMap<String,JobHashMapInfo>();
	KeyServerDataCenter()
	{
		
	}
	public synchronized static void JOB_INSERT(String JobId,String PartionId,String key)
	{
		JobHashMapInfo _tmpJob=JobMap.get(JobId);
		//System.out.println("Job_insert" + key + ":"  + PartionId);
		if(_tmpJob!=null)
			_tmpJob.Insert(PartionId, key);
		else
		{
			_tmpJob=new JobHashMapInfo(JobId);
			_tmpJob.Insert(PartionId, key);
			JobMap.put(JobId, _tmpJob);
		}
		
	}
	public synchronized static void JOB_INSERT_PARTION_LOCATION(String JobId,String PartionId,String Location)
	{
		JobHashMapInfo _tmpJob=JobMap.get(JobId);
		if(_tmpJob!=null)
			_tmpJob.InsertPartionLocation(PartionId, Location);
		else
		{
			_tmpJob=new JobHashMapInfo(JobId);
			_tmpJob.InsertPartionLocation(PartionId, Location);
			JobMap.put(JobId, _tmpJob);
		}
	}
	public synchronized static String JOB_GET_PATION_LIST(String JobId)
	{
		JobHashMapInfo _tmpJob=JobMap.get(JobId);
		String rtv=null;
		if(_tmpJob!=null)
		{
			rtv=_tmpJob.getPartionList();
			if(rtv!=null)
			{
				return rtv;
			}
			else
			{
    			return null;
			}
		}
		return null;
	}
	public synchronized static String JOB_GET_PATION_LOCATION_LIST(String JobId,String PartionId)
	{
		JobHashMapInfo _tmpJob=JobMap.get(JobId);
		if(_tmpJob!=null)
		{
			return _tmpJob.getPartionLocationList(PartionId);
		}
		return null;
	}
	public synchronized static String JOB_GET_KEY_LIST(String JobId,String PartionId)
	{
		JobHashMapInfo _tmpJob=JobMap.get(JobId);
		if(_tmpJob!=null)
		{
			return _tmpJob.getKeyList(PartionId);
		}
		return null;
	}
}
