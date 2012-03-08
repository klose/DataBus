package com.longyi.databus.daemon;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;
import org.zeromq.ZMQ.Context;

import com.longyi.databus.define.DATABUS;

public class PubMessageRecv extends Thread{
	private Context context;
    private final ZMQ.Poller poller;
	private final ZMQ.Socket SubSoc;
	private final DataMap dataMap;
    public PubMessageRecv(Context _context)
	{
		context=_context;
		this.poller = context.poller(1);
		SubSoc=context.socket(ZMQ.SUB);
		byte[] substring=new String("").getBytes();
		SubSoc.subscribe(substring);
		SubSoc.connect(DaemonMain.SubEndpoint);
		this.poller.register(SubSoc, ZMQ.Poller.POLLIN);
		dataMap=new DataMap();
	}
    @Override
	public void run()
	{
		while (!Thread.currentThread().isInterrupted()) 
		{
			try{
				if (poller.poll(250000) < 1) {
                    continue;
                }
				// process a request
	            if (poller.pollin(0))
	            {
	            	ZMsg UpdateMsg=ZMsg.recvMsg(SubSoc);
	            	
	            	int UpdateName=Integer.parseInt(UpdateMsg.pop().toString());
	            	String key=UpdateMsg.pop().toString();
	            	switch(UpdateName)
	            	{
	            	case DATABUS.SEND_MESSAGE:
	            		{
	            			String Location=UpdateMsg.pop().toString();
	            			if(!Location.equals(DaemonMain.LocalEndpoint))
	            				dataMap.freeMessage(key);
	            			dataMap.setMessageLocation(key, Location);
	            			break;
	            		}
	            	case DATABUS.FREE_MESSAGE:
	            		{
	            			dataMap.freeMessageLocation(key);
	            			dataMap.freeMessage(key);
	            			break;
	            		}
	            	case DATABUS.SEND_TO_CHANNEL:
		            	{
	            			String Location=UpdateMsg.pop().toString();
	            			if(!Location.equals(DaemonMain.LocalEndpoint))
	            				dataMap.freeChannel(key);
	            			dataMap.setChannelLocation(key, Location);
	            			break;
	            		}
	            	case DATABUS.FREE_CHANNEL:
		            	{
	            			dataMap.freeChannelLocation(key);
	            			dataMap.freeChannel(key);
	            			break;
	            		}
	            	case DATABUS.INSERT_A_FILE:
		            	{
	            			String Location=UpdateMsg.pop().toString();
	            			if(!Location.equals(DaemonMain.LocalEndpoint))
	            				dataMap.freeFile(key);
	            			dataMap.setFileLocation(key, Location);
	            			break;
	            		}
	            	case DATABUS.FREE_A_FILE:
		            	{
		            		dataMap.freeFileLocation(key);
	            			dataMap.freeFile(key);
	            			break;
	            		}
	            	case DATABUS.DELETE_A_FILE:
		            	{
		            		dataMap.freeFileLocation(key);
	            			dataMap.deleteFile(key);
	            			break;
	            		}
	            	case DATABUS.PUBSYS:
		            	{
		            		if(key.equals(DaemonMain.LocalEndpoint))
		            			DaemonMain.setSyncTrue();
		            		break;
		            	}
	            	default:
            			break;
	            	}
	            }
			 } catch (ZMQException e) {
	                // context destroyed, exit
	                if (ZMQ.Error.ETERM.getCode() == e.getErrorCode()) {
	                    break;
	                }
	                throw e;
	        }
		}
	}
}
