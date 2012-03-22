package com.longyi.databus.keyserver;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentSkipListSet;

public class PartionMapInfo {
	private String PartionId;
	private Set<String> KeyList;
	private Set<String> LocationList;

	PartionMapInfo(String PartionId)
	{
		this.PartionId=PartionId;
		KeyList=new ConcurrentSkipListSet<String>();
		//KeyList=new HashSet<String>();
		LocationList=new HashSet<String>();
	}
	public boolean Insert(String key)
	{
		if(KeyList.add(key))
		{
			return true;
		}
		else
			return false;
	}
	public boolean InsertPartionLocation(String Location)
	{
		return LocationList.add(Location);
	}
	public String getPartionLocationList()
	{
		return LocationList.toString();
	}
	public String getKeyList()
	{
		return KeyList.toString();
	}
}
