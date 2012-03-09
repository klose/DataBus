package com.longyi.databus.daemon;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQ.Socket;
import org.zeromq.ZMsg;

import com.longyi.databus.define.DATABUS;

public class UpdateToKeyServerThread extends Thread {
	private static Context context=DATABUS.context;
	private Socket ToServerSocket=null;
	private Socket LocSocket=null;
	UpdateToKeyServerThread()
	{
		ToServerSocket=context.socket(ZMQ.REQ);
		LocSocket=context.socket(ZMQ.PULL);
	}
	public void run()
	{
		while(!Thread.currentThread().isInterrupted())
		{
			ZMsg recvmsg=ZMsg.recvMsg(LocSocket);
			recvmsg.send(ToServerSocket);
			ZMsg.recvMsg(ToServerSocket);
		}
	}
}
