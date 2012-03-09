package com.longyi.databus.daemon;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import com.longyi.databus.define.DATABUS;
import com.longyi.databus.define.ValueObject;

public class PartionUpdateThread extends Thread {
	private class FetchThread extends Thread{
		private JobSocketService _jobSock=new JobSocketService();
		private String jobId;
		private String partionId;
		private String Location;
		private ArrayList<String> _keyList;
		private Socket backSoc;
		private Context context=DATABUS.context;
		private boolean ValueType;
		FetchThread(String jobId, String partionId, String Location,ArrayList<String> _keyList,boolean ValueType)
		{
			this.ValueType=ValueType;
			this.jobId=jobId;
			this.partionId=partionId;
			this.Location=Location;
			this._keyList=_keyList;
			backSoc=context.socket(ZMQ.PUSH);
			backSoc.connect("inproc://"+partionId+":13");
		}
		public void run()
		{
			int listSize=_keyList.size();
			ZMsg SendMsg=new ZMsg();
			SendMsg.addLast(Integer.toString(DATABUS.JOB_GET_KEY_BYTE));
			SendMsg.addLast(jobId);
			SendMsg.addLast(partionId);
			JobDataMap _tmpJobDataMap=DataMapForJob.JobDataMapFactory(jobId,ValueType);
			for(int i=0;i<listSize;i++)
			{
				ZMsg RealSendMsg=SendMsg.duplicate();
				RealSendMsg.addLast(_keyList.get(i));
				ZMsg backMsg=_jobSock.SendRequestOtherDaemon(Location, RealSendMsg);
				int ret=Integer.parseInt(backMsg.pop().toString());
				if(ret==DATABUS.SUCCESSFULLY)
				{
					if(ValueType==DATABUS.JOB_VALUE_BYTE)
					{
						List<byte[]> backList=new ArrayList<byte[]>();
						for(ZFrame tmpFrame:backMsg)
						{
							backList.add(tmpFrame.getData());
						}
						_tmpJobDataMap.putkeyByte(partionId, _keyList.get(i),backList);
					}
					else
					{
						List<ValueObject> backList=new ArrayList<ValueObject>();
						for(ZFrame tmpFrame:backMsg)
						{
							//backList.add(ValueObject(tmpFrame.getData()));
						}
						_tmpJobDataMap.putkeyObject(partionId, _keyList.get(i), backList);
					}
				}
				ZMsg SendToMasterThread=new ZMsg();
				SendToMasterThread.addLast(_keyList.get(i));
				//SendToMasterThread.addLast(Location);
				SendToMasterThread.send(backSoc);
			}
		}
	}
	private static Context context=DATABUS.context;
	private static final HashMap<String,ZMQ.Socket> OuterServerMap=new HashMap<String,ZMQ.Socket>();
	private Socket _socketToTask=null;
	private Socket _RecFromThread=null;
	private String jobId;
	private String partionId;
	private String[] Location;
	private ArrayList<String> _keyList;
	private int[] state=null;
	private Socket _socketRecvTask=null;
	private boolean ThreadState=true;
	private int AlreadygetNumber;
	PartionUpdateThread(String jobId,String partionId,String[] Location,ArrayList<String> _keyList,boolean ValueType)
	{
		this.jobId=jobId;
		this.partionId=partionId;
		this.Location=Location;
		this._keyList=_keyList;
		AlreadygetNumber=_keyList.size();
		ThreadState=true;
		state=new int[_keyList.size()];
		_RecFromThread=context.socket(ZMQ.PULL);
		_RecFromThread.bind("inproc://"+partionId+":13");
		_socketToTask=context.socket(ZMQ.PUSH);
		_socketToTask.bind("inproc://"+partionId);
		_socketRecvTask=context.socket(ZMQ.PULL);
		_socketRecvTask.connect("inproc://"+partionId);
		for(int i=0;i<Location.length;i++)
		{
			Thread tmp=new FetchThread(jobId,partionId,Location[i],_keyList,ValueType);
			tmp.start();
		}
	}
	public void run()
	{
		int finishNumber=0;
		while(!Thread.currentThread().isInterrupted())
		{
			ZMsg recvMsg=ZMsg.recvMsg(_RecFromThread);
			ZMsg MsgToTask=recvMsg.duplicate();
			String key=recvMsg.pop().toString();
			int index=_keyList.indexOf(key);
			if(index!=-1){
				state[index]++;
				if(state[index]==Location.length)
				{
					finishNumber++;
					MsgToTask.send(_socketToTask);
				}
			}
			if(finishNumber==_keyList.size())
				break;
		}
		while(ThreadState)
		{
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	public synchronized String getPrepareKey()
	{
		if(AlreadygetNumber==0)
			return null;
		ZMsg recvMsg=ZMsg.recvMsg(_socketRecvTask);
		AlreadygetNumber--;
		return recvMsg.pop().toString();
	}
	public void destroyPartionUpdate()
	{
		ThreadState=false;
	}
	public ArrayList<String> getKeyList()
	{
		return _keyList;
	}
}
