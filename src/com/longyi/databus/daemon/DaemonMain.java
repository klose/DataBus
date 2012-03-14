package com.longyi.databus.daemon;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMQQueue;
import org.zeromq.ZMsg;

import com.longyi.databus.define.DATABUS;
import com.longyi.databus.define.GetLocalIpAddress;
import com.longyi.databus.daemon.DataMap;
public class DaemonMain extends Thread{

	public static String LocalEndpoint;
	
	public static String KeyServerEndpoint;
	public static String SubEndpoint;
	public final static int SubEndpointPort = 34521;
	public final static int KeyServerEndpointPort = 34520;
	public String KeyServerIpAddr = "";
	public static String LocalIpAddress;
	public final static int InnerWorkThreadNum = 5;
	public final static int OuterWorkThreadNum = 5;
	private static Context context=DATABUS.context;
	private static Socket ToKeyServerSoc;
	private static Socket InnerSoc;
	private static Socket OuterSoc;
	private static Socket InbackendSoc;
	private static Socket OuterbackendSoc;
	private static volatile boolean sync;
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
	public DaemonMain(String serverIpAddr)
	{
		try {
			this.KeyServerIpAddr = InetAddress.getByName(serverIpAddr).getHostAddress();
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("DaemonMain: KeyServerIpAddr " + KeyServerIpAddr);
		KeyServerEndpoint = "tcp://" + this.KeyServerIpAddr + ":" + this.KeyServerEndpointPort;
		SubEndpoint =  "tcp://" + this.KeyServerIpAddr + ":" + this.SubEndpointPort;
		File msgTmpDir = new File(DATABUS.FILE_DATA_PATH + "/Message");
		File chTmpDir = new File(DATABUS.FILE_DATA_PATH + "/Channel");
		if (!msgTmpDir.exists()) {
			msgTmpDir.mkdirs();
		}
		if (!chTmpDir.exists()) {
			chTmpDir.mkdirs();
		}
		setSyncFalse();
		//context=ZMQ.context(10);
		LocalIpAddress=GetLocalIpAddress.getIpAddresses();
		if(LocalIpAddress==null)
			System.out.println("Cat get local ip address");
		
		LocalEndpoint="tcp://"+LocalIpAddress+":9988";
		//固定地址
		
		ToKeyServerSoc=context.socket(ZMQ.REQ);
		ToKeyServerSoc.connect(KeyServerEndpoint);
		InnerSoc=context.socket(ZMQ.ROUTER);
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
		//Thread _updateThread=new UpdateToKeyServerThread();
		//_updateThread.start();
	}
	
	private static boolean getAllInfoFromKeyServer()
	{
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
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		DataMap dataMap=new DataMap();
		ZMsg RequestMsg=new ZMsg();
		RequestMsg.addLast(Integer.toString(DATABUS.GET_ALL_KEY_INFO));
		RequestMsg.send(ToKeyServerSoc);
		ZMsg RecvMsg=ZMsg.recvMsg(ToKeyServerSoc);
		dataMap.creatMessageLocationMap(RecvMsg.pop().toString());
		dataMap.creatChannelLocationMap(RecvMsg.pop().toString());
		dataMap.creatFileLocationMap(RecvMsg.pop().toString());
		
		return true;
	}
	public void run() {
		getAllInfoFromKeyServer();
		System.out.println("Start DataBus Daemon...");
		for(int i=0;i<InnerWorkThreadNum;i++)
		{
			InnerWorkThread _iner=new InnerWorkThread(context);
			_iner.start();
		}
		for(int j=0;j<InnerWorkThreadNum;j++)
		{
			OuterWorkThread _outer=new OuterWorkThread(context);
			_outer.start();
		}
		
//		while(!currentThread().isInterrupted())
//		{
//			try {
//				Thread.sleep(5000);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
	}

}
