package com.longyi.databus.keyserver;

import java.util.HashSet;
import java.util.Set;

public class PartionMapInfo {
	private String PartionId;
	private Set<String> KeyList;
	private Set<String> LocationList;
	PartionMapInfo(String PartionId)
	{
		this.PartionId=PartionId;
		KeyList=new HashSet<String>();
		LocationList=new HashSet<String>();
	}
	public boolean Insert(String key)
	{
		return KeyList.add(key);
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
