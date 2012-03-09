package com.longyi.databus.keyserver;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class JobHashMapInfo {
	private String jobId=null;
	private HashMap<String,PartionMapInfo> JobInfoMap=null;
	private Set<String> PartionList=null;
	public JobHashMapInfo(String jobId)
	{
		this.jobId=jobId;
		JobInfoMap=new HashMap<String,PartionMapInfo>();
		PartionList=new HashSet<String>();
	}
	public int Insert(String PartionId,String key)
	{
		PartionMapInfo _tmpPartionMapInfo=JobInfoMap.get(PartionId);
		if(_tmpPartionMapInfo!=null)
		{
			_tmpPartionMapInfo.Insert(key);
		}
		else
		{
			_tmpPartionMapInfo=new PartionMapInfo(jobId);
			JobInfoMap.put(PartionId, _tmpPartionMapInfo);
			_tmpPartionMapInfo.Insert(key);
		}
		return 1;
	}
	public int InsertPartionLocation(String PartionId,String Location)
	{
		PartionMapInfo _tmpPartionMapInfo=JobInfoMap.get(PartionId);
		if(_tmpPartionMapInfo!=null)
		{
			_tmpPartionMapInfo.InsertPartionLocation(Location);
		}
		else
		{
			_tmpPartionMapInfo=new PartionMapInfo(jobId);
			JobInfoMap.put(PartionId, _tmpPartionMapInfo);
			_tmpPartionMapInfo.InsertPartionLocation(Location);
		}
		return 1;
	}
	public String getPartionList()
	{
		return PartionList.toString();
	}
	public String getPartionLocationList(String PartionId)
	{
		PartionMapInfo _tmpPartionMapInfo=JobInfoMap.get(PartionId);
		if(_tmpPartionMapInfo!=null)
			return _tmpPartionMapInfo.getPartionLocationList();
		else
			return null;
	}
	public String getKeyList(String PartionId)
	{
		PartionMapInfo _tmpPartionMapInfo=JobInfoMap.get(PartionId);
		if(_tmpPartionMapInfo!=null)
			return _tmpPartionMapInfo.getKeyList();
		else
			return null;
	}
	
}
