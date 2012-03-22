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
			backSoc.connect("inproc://"+jobId+partionId+":13");
		}
		public void run()
		{
			int listSize=_keyList.size();
			
			ZMsg SendMsg=new ZMsg();
			if(ValueType==DATABUS.JOB_VALUE_BYTE)
				SendMsg.addLast(Integer.toString(DATABUS.JOB_GET_KEY_BYTE));
			else
				SendMsg.addLast(Integer.toString(DATABUS.JOB_GET_KEY_OBJECT));
			SendMsg.addLast(jobId);
			SendMsg.addLast(partionId);
			JobDataMap _tmpJobDataMap=DataMapForJob.JobDataMapFactory(jobId,ValueType,null);
			for(int i=0;i<listSize;i++)
			{
				ZMsg RealSendMsg=SendMsg.duplicate();
				RealSendMsg.addLast(_keyList.get(i));
				//System.out.println("heihie");
				ZMsg backMsg=_jobSock.SendRequestOtherDaemon(Location, RealSendMsg);
				//System.out.println("heihie");
				int ret=Integer.parseInt(backMsg.pop().toString());
				if(ret==DATABUS.SUCCESSFULLY)
				{
					if(ValueType==DATABUS.JOB_VALUE_BYTE)
					{
						List<byte[]> backList=new ArrayList<byte[]>();
						if(backMsg.size()!=0)
						{
							for(ZFrame tmpFrame:backMsg)
							{
								backList.add(tmpFrame.getData());
							}
							_tmpJobDataMap.putkeyByte(partionId, _keyList.get(i),backList);
						}
					}
					else
					{
						List<Object> backList=new ArrayList<Object>();
						if(backMsg.size()!=0)
						{
							for(ZFrame tmpFrame:backMsg)
							{
								backList.add(JobDataMap.getObject(tmpFrame.getData()));
							}
							_tmpJobDataMap.putkeyObject(partionId, _keyList.get(i), backList);
						}
					}
				}
				ZMsg SendToMasterThread=new ZMsg();
				SendToMasterThread.addLast(_keyList.get(i));
				
				SendToMasterThread.send(backSoc);
			}
		}
	}
	private static Context context=DATABUS.context;
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
	private int ThreadNumber=0;
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
		_RecFromThread.bind("inproc://"+jobId + partionId+":13");
		_socketToTask=context.socket(ZMQ.PUSH);
		_socketToTask.bind("inproc://"+ jobId + partionId);
		_socketRecvTask=context.socket(ZMQ.PULL);
		_socketRecvTask.connect("inproc://"+ jobId + partionId);
		for(int i=0;i<Location.length;i++)
		{
			//System.out.println(Location[i]);
			//System.out.println(DaemonMain.LocalEndpoint);
			if(!Location[i].equals(DaemonMain.LocalEndpoint))
			{
				Thread tmp=new FetchThread(jobId,partionId,Location[i],_keyList,ValueType);
				tmp.start();
				ThreadNumber++;
			}
			else
			{
				for(int j=0;j<state.length;j++)
					state[j]=1;
			}
		}
	}
	public void run()
	{
		int finishNumber=0;
		//System.out.println("finishNumber="+finishNumber);
		while(!Thread.currentThread().isInterrupted())
		{
			//System.out.println("ThreadNumber="+ThreadNumber);
			if(ThreadNumber>0)
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
			else
			{
				for(int i=0;i<_keyList.size();i++)
				{
					ZMsg preparekey=new ZMsg();
					preparekey.addLast(_keyList.get(i));
					preparekey.send(_socketToTask);
					//System.out.println("============");
				}
				break;
			}
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
	public String getPrepareKey()
	{
		//System.out.println("getPrepareKey "+partionId+" ");
		if(AlreadygetNumber==0)
		{
			ThreadState=false;
			return null;
		}
		ZMsg recvMsg=ZMsg.recvMsg(_socketRecvTask);
		AlreadygetNumber--;
		//System.out.println("getPrepareKey key="+recvMsg.peekFirst().toString());
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
