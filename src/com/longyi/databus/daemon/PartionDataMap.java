package com.longyi.databus.daemon;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;




public class PartionDataMap {
	private String partionId=null;
	private HashMap<String,List<Object> > ObjectMap=null;
	private HashMap<String,List<byte[]> > ByteMap=null;
	private HashMap<String,Integer> TestMap=null;
	private String combineClsName = null;
	private Combiner combinerInstance;
	public PartionDataMap(String partionId)
	{
		this(partionId, null);
	}
	public PartionDataMap(String partionId, String combineClsName) {
		
		this.partionId=partionId;
		this.ObjectMap=new HashMap<String,List<Object> >();
		this.ByteMap=new HashMap<String,List<byte[]> >();
		this.TestMap=new HashMap<String,Integer>();
		this.combineClsName = combineClsName;
		if (this.combineClsName != null) {
			try {
				Class combineCls = Class.forName(this.combineClsName);
				Constructor<Combiner> meth = (Constructor<Combiner>) combineCls
						.getConstructor(new Class[0]);
				meth.setAccessible(true);
				this.combinerInstance = meth.newInstance();
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchMethodException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
				
		}
	}
	public synchronized boolean putkeyObject(String key,List<Object> value)
	{
		List<Object> _listObject=ObjectMap.get(key);
		if(_listObject==null)
		{
			_listObject=this.combinerInstance.combine(key, value);
//			_listObject=new CopyOnWriteArrayList<Object>();
//			_listObject.addAll(value);
			ObjectMap.put(key, _listObject);
			return false;
		}
		else
		{
			_listObject.addAll(value);
			_listObject=this.combinerInstance.combine(key, _listObject);
			return true;
		}
	}
	
	public synchronized boolean putkeyByte(String key,List<byte[]> value)
	{
		Integer test=TestMap.get(key);
		if(test == null)
		{
			test=Integer.parseInt(new String(value.get(0)));
			TestMap.put(key,test);
			return false;
		}
		else
		{
			test+=Integer.parseInt(new String(value.get(0)));
			//System.out.println("+_+_+_+_+_+_+_+_+_+_+_+"+test);
			TestMap.put(key, test);
			return true;
		}
	}
	public synchronized List<byte[]> getkeyByte(String key)
	{
		List<byte[]> _rtv=new LinkedList<byte[]>();
		Integer test=TestMap.get(key);
		if(test==null)
		{
			return null;
			//_rtv.add(Integer.toString(test).getBytes());
			//return _rtv;
		}
		else
		{
			_rtv.add(Integer.toString(test).getBytes());
			return _rtv;
		}
	}
	
	/*
	public synchronized boolean putkeyByte(String key,List<byte[]> value)
	{
		List<byte[]> _listByte=ByteMap.get(key);
		if(_listByte==null)
		{
			_listByte = new CopyOnWriteArrayList<byte[]>();
			_listByte.addAll(value);
			ByteMap.put(key, _listByte);
			return false;
		}
		else
		{
			_listByte.addAll(value);
			return true;
		}
	}
	*/
	public synchronized List<Object> getkeyObject(String key)
	{
		return ObjectMap.get(key);
	}
	/*
	public synchronized List<byte[]> getkeyByte(String key)
	{
		return ByteMap.get(key);
	}
	*/
}
