package com.longyi.databus.daemon;

import java.util.HashMap;
import java.util.List;

import com.longyi.databus.define.ValueObject;

public class PartionDataMap {
	private String partionId=null;
	private HashMap<String,List<ValueObject> > ObjectMap=null;
	private HashMap<String,List<byte[]> > ByteMap=null;
	public PartionDataMap(String partionId)
	{
		this.partionId=partionId;
		ObjectMap=new HashMap<String,List<ValueObject> >();
		ByteMap=new HashMap<String,List<byte[]> >();
	}
	public boolean putkeyObject(String key,List<ValueObject> value)
	{
		List<ValueObject> _listObject=ObjectMap.get(key);
		if(_listObject==null)
		{
			ObjectMap.put(key, value);
			return false;
		}
		else
		{
			for(ValueObject tmpObject:value)
				_listObject.add(tmpObject);
			return true;
		}
	}
	public boolean putkeyByte(String key,List<byte[]> value)
	{
		List<byte[]> _listByte=ByteMap.get(key);
		if(_listByte==null)
		{
			ByteMap.put(key, value);
			return false;
		}
		else
		{
			for(byte[] tmpByte:value)
				_listByte.add(tmpByte);
			return true;
		}
	}
	public List<ValueObject> getkeyObject(String key)
	{
		return ObjectMap.get(key);
	}
	public List<byte[]> getkeyByte(String key)
	{
		return ByteMap.get(key);
	}
}
