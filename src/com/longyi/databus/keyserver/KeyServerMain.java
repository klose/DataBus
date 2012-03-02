package com.longyi.databus.keyserver;

import java.util.HashMap;

import org.zeromq.ZMQ;
import org.zeromq.ZMQForwarder;
import org.zeromq.ZMQQueue;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;

import com.longyi.databus.daemon.GetLocalIpAddress;
import com.longyi.databus.define.DATABUS;

public class KeyServerMain {
	public static final HashMap<String,String> MessageMap=new HashMap<String,String>();
	public static final HashMap<String,String> ChannelMap=new HashMap<String,String>();
	public static final HashMap<String,String> FileMap=new HashMap<String,String>();
	private static Context context;
	private static ZMQ.Socket pubSoc;
	private static ZMQ.Socket reqSoc;
	private static ZMQ.Socket reqbackendSoc;
	private static ZMQ.Socket pubbackendSoc;
	
	private static String LocalIpAddress;
	
	private static String pubEndpoint;
	private static String reqEndpoint;
	/**
	 * @param args
	 */
	KeyServerMain()
	{
		LocalIpAddress=GetLocalIpAddress.getIpAddresses();
		if(LocalIpAddress==null)
			System.out.println("Can not get Local IP Address");
		
		context=ZMQ.context(10);
		reqSoc=context.socket(ZMQ.ROUTER);
		
		reqEndpoint="tcp://"+LocalIpAddress+":34520";
		reqSoc.bind(reqEndpoint);
		pubSoc=context.socket(ZMQ.PUB);
		
		pubEndpoint="tcp://"+LocalIpAddress+":34521";
		pubSoc.bind(pubEndpoint);
		reqbackendSoc=context.socket(ZMQ.DEALER);
		reqbackendSoc.bind(DATABUS.KEYREQBACKEND);
		pubbackendSoc=context.socket(ZMQ.PULL);
		pubbackendSoc.bind(DATABUS.KEYPUBBACKEND);
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		KeyServerMain _ins=new KeyServerMain();
		Thread ReqThread = new Thread(new ZMQQueue(context, reqSoc, reqbackendSoc));
		ReqThread.start();
        Thread PubThread = new Thread(new ZMsgForThread(context, pubbackendSoc,pubSoc));
        PubThread.start();
		
		for(int i=0;i<5;i++){
			KeyServerWorkThread _workThread=new KeyServerWorkThread(context);
			_workThread.start();
		}
		
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
