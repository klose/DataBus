package com.longyi.databus.daemon;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQQueue;
import org.zeromq.ZMsg;

import com.longyi.databus.define.DATABUS;

public class DaemonMain {

	public static String LocalEndpoint;
	
	public static String KeyServerEndpoint="tcp://10.10.102.17:34520";
	public static String SubEndpoint="tcp://10.10.102.17:34521";
	public static String LocalIpAddress;
	
	private static Context context;
	private static Socket ToKeyServerSoc;
	private static Socket InnerSoc;
	private static Socket OuterSoc;
	private static Socket InbackendSoc;
	private static Socket OuterbackendSoc;
	private static boolean sync;
	/**
	 * @param args
	 */
	public static void setSyncTrue()
	{
		sync=true;
	}
	public static void setSyncFalse()
	{
		sync=false;
	}
	DaemonMain()
	{
		setSyncFalse();
		context=ZMQ.context(10);
		LocalIpAddress=GetLocalIpAddress.getIpAddresses();
		if(LocalIpAddress==null)
			System.out.println("Cat get local ip address");
		
		LocalEndpoint="tcp://"+LocalIpAddress+":9987";
		//固定地址
		
		ToKeyServerSoc=context.socket(ZMQ.REQ);
		ToKeyServerSoc.connect(KeyServerEndpoint);
		InnerSoc=context.socket(ZMQ.ROUTER);
//		InnerSoc.bind(DATABUS.LOCAL_JAVA_DAEMON_ENDPOINT);
		InnerSoc.bind(DATABUS.LOCAL_JAVA_DAEMON_ENDPOINT + LocalIpAddress.substring(LocalIpAddress.length()-2) + ":" + DATABUS.ENDPOINT_PORT);
		OuterSoc=context.socket(ZMQ.ROUTER);
		OuterSoc.bind(LocalEndpoint);
		InbackendSoc=context.socket(ZMQ.DEALER);
		InbackendSoc.bind(DATABUS.INBACKEND);
		OuterbackendSoc=context.socket(ZMQ.DEALER);
		OuterbackendSoc.bind(DATABUS.OUTBACKEND);
		Thread InReqThread = new Thread(new ZMQQueue(context, OuterSoc, OuterbackendSoc));
		InReqThread.start();
		Thread OutReqThread = new Thread(new ZMQQueue(context, InnerSoc, InbackendSoc));
		OutReqThread.start();
	}
	private static boolean getAllInfoFromKeyServer()
	{
		DataMap dataMap=new DataMap();
		ZMsg RequestMsg=new ZMsg();
		RequestMsg.addLast(Integer.toString(DATABUS.GET_ALL_KEY_INFO));
		RequestMsg.send(ToKeyServerSoc);
		ZMsg RecvMsg=ZMsg.recvMsg(ToKeyServerSoc);
		dataMap.creatMessageLocationMap(RecvMsg.pop().toString());
		dataMap.creatChannelLocationMap(RecvMsg.pop().toString());
		dataMap.creatFileLocationMap(RecvMsg.pop().toString());
		
		PubMessageRecv _pub=new PubMessageRecv(context);
		_pub.start();
		while(!sync)
		{
			ZMsg SyncMsg=new ZMsg();
			SyncMsg.addLast(Integer.toString(DATABUS.PUBSYS));
			SyncMsg.addLast(LocalEndpoint);
			SyncMsg.send(ToKeyServerSoc);
			ZMsg SyncReceive=ZMsg.recvMsg(ToKeyServerSoc);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		return true;
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DaemonMain _daemon=new DaemonMain();	
		getAllInfoFromKeyServer();
		for(int i=0;i<5;i++)
		{
			InnerWorkThread _iner=new InnerWorkThread(context);
			_iner.start();
		}
		for(int j=0;j<5;j++)
		{
			OuterWorkThread _outer=new OuterWorkThread(context);
			_outer.start();
		}
		System.out.println("Set Up Ok");
		while(!Thread.currentThread().isInterrupted())
		{
			try {
				Thread.sleep(5000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
