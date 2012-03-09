package com.longyi.databus.daemon;

import java.util.HashMap;

public class DataMapForJob {
	private static final HashMap<String,JobDataMap> JobMap=new HashMap<String,JobDataMap>();
	public DataMapForJob()
	{
	}
	public static JobDataMap JobDataMapFactory(String jobId,boolean ValueType)
	{
		JobDataMap _tmpJobDataMap=JobMap.get(jobId);
		if(_tmpJobDataMap==null)
		{
			_tmpJobDataMap=new JobDataMap(jobId,ValueType);
			JobMap.put(jobId, _tmpJobDataMap);
		}
		return _tmpJobDataMap;
	}
}
