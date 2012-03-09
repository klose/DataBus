package com.longyi.databus.daemon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;
import org.zeromq.ZMQ.Context;

import com.longyi.databus.define.DATABUS;
import com.longyi.databus.define.ValueObject;

public class JobDataMap {
	private String jobId=null;
	private HashMap<String,PartionDataMap> jobDataMap=null;
	private JobSocketService _jobSocketServer;
	private static final Context context=DATABUS.context;
	private Socket _socketToUpdateTokeyServer=null;
	private boolean ValueType=true;
	public JobDataMap(String jobId,boolean ValueType)
	{
		this.jobId=jobId;
		this.ValueType=ValueType;
		jobDataMap=new HashMap<String,PartionDataMap>();
		_jobSocketServer=new JobSocketService();
		_socketToUpdateTokeyServer=context.socket(ZMQ.PUSH);
		_socketToUpdateTokeyServer.connect(DATABUS.JOB_UPDATE_END);
	}
	public PartionDataMap PartionDataMapFactory(String partionId)
	{
		PartionDataMap _tmpPartionDataMap=jobDataMap.get(partionId);
		if(_tmpPartionDataMap==null)
		{
			_tmpPartionDataMap=new PartionDataMap(partionId);
			jobDataMap.put(partionId, _tmpPartionDataMap);
			updatePartionInfoToKeyServer(partionId);
		}
		return _tmpPartionDataMap;
	};
	//这里搞成异步才行
	private void updatePartionInfoToKeyServer(String partionId)
	{
		ZMsg updateMsg=new ZMsg();
		updateMsg.addLast(Integer.toString(DATABUS.JOB_INSERT_PATION_LOCATION));
		updateMsg.addLast(jobId);
		updateMsg.addLast(partionId);
		updateMsg.addLast(DaemonMain.KeyServerEndpoint);
		updateMsg.send(_socketToUpdateTokeyServer);
	}
	private void updateKeyInfoToKeyServer(String partionId,String key)
	{
		ZMsg updateMsg=new ZMsg();
		updateMsg.addLast(Integer.toString(DATABUS.JOB_INSERT));
		updateMsg.addLast(jobId);
		updateMsg.addLast(partionId);
		updateMsg.addLast(key);
		updateMsg.send(_socketToUpdateTokeyServer);
	}
	public boolean putkeyObject(String partId,String key,List<ValueObject> value)
	{
		PartionDataMap _tmpPartionDataMap=PartionDataMapFactory(partId);
		if(_tmpPartionDataMap.putkeyObject(key,value))
			return true;
		else
		{
			updateKeyInfoToKeyServer(partId,key);
			return true;
		}
	}
	
	public boolean putkeyByte(String partId,String key,List<byte[]> value)
	{
		PartionDataMap _tmpPartionDataMap=PartionDataMapFactory(partId);
		if(_tmpPartionDataMap.putkeyByte(key,value))
			return true;
		else
		{
			updateKeyInfoToKeyServer(partId,key);
			return true;
		}
	}
	
	
	public List<ValueObject> getkeyObject(String partId,String key)
	{
		PartionDataMap _tmpPartionDataMap=jobDataMap.get(partId);
		if(_tmpPartionDataMap!=null)
			return _tmpPartionDataMap.getkeyObject(key);
		else
			return null;
	}
	
	public List<byte[]> getkeyByte(String partId,String key)
	{
		PartionDataMap _tmpPartionDataMap=jobDataMap.get(partId);
		if(_tmpPartionDataMap!=null)
			return _tmpPartionDataMap.getkeyByte(key);
		else
			return null;
	}
	
	public ArrayList<String> getkeyList(String partId)
	{
		ZMsg msgSend=new ZMsg();
		msgSend.addLast(Integer.toString(DATABUS.JOB_GET_KEY_LIST));
		msgSend.addLast(jobId);
		msgSend.addLast(partId);
		ZMsg msgRecv=_jobSocketServer.SendRequestToKeyServer(msgSend);
		int rtv=Integer.parseInt(msgRecv.pop().toString());
		if(rtv==DATABUS.SUCCESSFULLY)
		{
			String keylist=msgRecv.pop().toString();
			String[] arraykey=keylist.split(", ");
			ArrayList<String> backvalue=new ArrayList<String>();
			int length=arraykey.length;
			backvalue.add(arraykey[0].substring(1));
			for(int i=1;i<length-1;i++)
				backvalue.add(arraykey[i]);
			backvalue.add(arraykey[length-1].substring(0, arraykey[length-1].length()-2));
			return backvalue;
		}
		else
			return null;
	}
	
	public PartionUpdateThread update(String partId)
	{
		ArrayList<String> _keyList=getkeyList(partId);
		ZMsg msgSend=new ZMsg();
		msgSend.addLast(Integer.toString(DATABUS.JOB_GET_PATION_LOCATION_LIST));
		msgSend.addLast(jobId);
		msgSend.addLast(partId);
		ZMsg msgRecv=_jobSocketServer.SendRequestToKeyServer(msgSend);
		int rtv=Integer.parseInt(msgRecv.pop().toString());
		if(rtv==DATABUS.SUCCESSFULLY)
		{
			String LocationList=msgRecv.pop().toString();
			String[] arraykey=LocationList.split(", ");
			arraykey[0]=arraykey[0].substring(1);
			int length=arraykey.length;
			arraykey[length-1]=arraykey[length-1].substring(0, arraykey[length-1].length()-2);
			PartionUpdateThread updateThread=new PartionUpdateThread(jobId,partId,arraykey,_keyList,ValueType);
			updateThread.start();
			return updateThread;
		}
		return null;
	}
}
