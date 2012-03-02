package com.longyi.databus.daemon;
import java.util.HashMap;
import java.util.Iterator;

import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;

import com.longyi.databus.define.DATABUS;


public class InnerWorkThread extends Thread{
	private Context context;
    private final ZMQ.Poller poller;
	private final ZMQ.Socket worker;
	private final ZMQ.Socket SocToKeyServer;
	private final HashMap<String,ZMQ.Socket> OuterServerMap;
	private final DataMap dataMap;
    public InnerWorkThread(Context _context)
	{
		context=_context;
		this.poller = context.poller(1);
        worker=context.socket(ZMQ.DEALER);
		worker.connect(DATABUS.INBACKEND);
		dataMap=new DataMap();
		SocToKeyServer=context.socket(ZMQ.REQ);
		SocToKeyServer.connect(DaemonMain.KeyServerEndpoint);
		OuterServerMap=new HashMap<String,ZMQ.Socket>();
		this.poller.register(worker, ZMQ.Poller.POLLIN);
	}
    private ZMsg appendMsg(ZMsg Target,ZMsg Src)
    {
    	Iterator<ZFrame> i=Src.iterator();
    	while(i.hasNext()) {
            ZFrame f = i.next();
            Target.addLast(f);
    	}
    	return Target;
    };
    private ZMsg RequestToKeyServer(ZMsg RequestMsg)
    {
    	RequestMsg.send(SocToKeyServer);
    	return ZMsg.recvMsg(SocToKeyServer);
    }
    private ZMsg RequestToOuterNode(ZMsg RequestMsg,String Endpoint)
    {
    	ZMQ.Socket OuterServer=OuterServerMap.get(Endpoint);
    	if(OuterServer==null)
    	{
    		OuterServer=context.socket(ZMQ.REQ);
    		OuterServer.connect(Endpoint);
    		OuterServerMap.put(Endpoint, OuterServer);
    	}
    	RequestMsg.send(OuterServer);
		return ZMsg.recvMsg(OuterServer);
    }
    @Override
	public void run()
	{
		while (!Thread.currentThread().isInterrupted()) 
		{
			try{
				if (poller.poll() < 1) {
                    continue;
                }
				// process a request
	            if (poller.pollin(0))
	            {
	            	ZMsg RequestMsg=ZMsg.recvMsg(worker);
	            	ZMsg BackMsg=new ZMsg();
	            	BackMsg.wrap(RequestMsg.unwrap());
	            	int RequestName=Integer.parseInt(RequestMsg.pop().toString());
	            	switch(RequestName)
	            	{
	            	case DATABUS.GET_MESSAGE:
	            		{
	            			String key=RequestMsg.pop().toString();
	            			ZMsg rtv=dataMap.getMessage(key);
	            			if(rtv!=null){//数据在本地
	            				BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	            				appendMsg(BackMsg,rtv).send(worker);
	            			}
	            			else//数据不在本地
	            			{
	            				String Endpoint=dataMap.getMessageLocation(key);
	            				if(Endpoint==null)
	            				{
		            				BackMsg.addLast(Integer.toString(DATABUS.FAILED));
		            				BackMsg.send(worker);
	            				}
	            				else
	            				{
	            					ZMsg ToOuterNode=new ZMsg();
	            					ToOuterNode.addLast(Integer.toString(DATABUS.GET_MESSAGE));
	            					ToOuterNode.addLast(key);
	            					ZMsg RecvMsg=RequestToOuterNode(ToOuterNode, Endpoint);
	            					appendMsg(BackMsg,RecvMsg).send(worker);
	            				}
	            			}
	            			break;
	            		}
	            	case DATABUS.GET_MESSAGE_LOCATION:
		            	{
	            			String key=RequestMsg.pop().toString();
	            			if(dataMap.IsMessageExist(key))
	            			{
	            				BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	            				BackMsg.addLast(DaemonMain.LocalEndpoint);
	            				BackMsg.send(worker);
	            			}
	            			else
	            			{
	            				String Endpoint=dataMap.getMessageLocation(key);
	            				if(Endpoint!=null)
	            				{
	            					BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	            					BackMsg.addLast(Endpoint);
	            					BackMsg.send(worker);
	            				}
	            				else
	            				{
	            					BackMsg.addLast(Integer.toString(DATABUS.FAILED));
	            					BackMsg.send(worker);
	            				}
	            			}
	            			break;
		            	}
	            	case DATABUS.SEND_MESSAGE:
	            		{
	            			String key=RequestMsg.pop().toString();
	            			dataMap.storeMessage(key, RequestMsg, RequestMsg.contentSize());
	            			BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
        					BackMsg.send(worker);
        					
        					ZMsg ToKeyServer=new ZMsg();
        					ToKeyServer.addLast(Integer.toString(DATABUS.SEND_MESSAGE));
        					ToKeyServer.addLast(key);
        					ToKeyServer.addLast(DaemonMain.LocalEndpoint);
        					ZMsg UpdateResMsg=RequestToKeyServer(ToKeyServer);
        					if(UpdateResMsg.pop().streq(Integer.toString(DATABUS.SUCCESSFULLY)))
        						;
        					else
        						System.out.println("SEND_MESSAGE update to key server failed key="+key);
        					break;
	            		}
	            	case DATABUS.FREE_MESSAGE:
	            		{
	            			String key=RequestMsg.pop().toString();
	            			dataMap.freeMessage(key);
	            			
	            			BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
        					BackMsg.send(worker);
        					
        					ZMsg ToKeyServer=new ZMsg();
        					ToKeyServer.addLast(Integer.toString(DATABUS.FREE_MESSAGE));
        					ToKeyServer.addLast(key);
        					ZMsg UpdateResMsg=RequestToKeyServer(ToKeyServer);
        					if(UpdateResMsg.pop().streq(Integer.toString(DATABUS.SUCCESSFULLY)))
        						;
        					else
        						System.out.println("FREE_MESSAGE update to key server failed key="+key);
        					break;
	            		}
	            	case DATABUS.GET_CHANNEL_LOCATION:
		            	{
	            			String key=RequestMsg.pop().toString();
	            			
	            			if(dataMap.IsChannelExist(key))
	            			{
	            				BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	            				BackMsg.addLast(DaemonMain.LocalEndpoint);
	            				BackMsg.send(worker);
	            			}
	            			else
	            			{
	            				String Endpoint=dataMap.getChannelLocation(key);
	            				if(Endpoint!=null)
	            				{
	            					BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	            					BackMsg.addLast(Endpoint);
	            					BackMsg.send(worker);
	            				}
	            				else
	            				{
	            					BackMsg.addLast(Integer.toString(DATABUS.FAILED));
	            					BackMsg.send(worker);
	            				}
	            			}
	            			break;
		            	}
	            	case DATABUS.SEND_TO_CHANNEL:
		            	{
	            			String key=RequestMsg.pop().toString();
	            			dataMap.storeChannelData(key, RequestMsg, RequestMsg.contentSize());
	            			BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	    					BackMsg.send(worker);
	    					
	    					ZMsg ToKeyServer=new ZMsg();
	    					ToKeyServer.addLast(Integer.toString(DATABUS.SEND_TO_CHANNEL));
	    					ToKeyServer.addLast(key);
	    					ToKeyServer.addLast(DaemonMain.LocalEndpoint);
	    					ZMsg UpdateResMsg=RequestToKeyServer(ToKeyServer);
	    					if(UpdateResMsg.pop().streq(Integer.toString(DATABUS.SUCCESSFULLY)))
	    						;
	    					else
	    						System.out.println("SEND_TO_CHANNEL update to key server failed key="+key);
	    					break;
		            	}
	            	case DATABUS.GET_FROM_CHANNEL:
		            	{
	            			String key=RequestMsg.pop().toString();
	            			ZMsg rtv=dataMap.getAllChannelData(key);
	            			if(rtv!=null){//数据在本地
	            				BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	            				appendMsg(BackMsg,rtv).send(worker);
	            			}
	            			else//数据不在本地
	            			{
	            				String Endpoint=dataMap.getChannelLocation(key);
	            				if(Endpoint==null)
	            				{
		            				BackMsg.addLast(Integer.toString(DATABUS.FAILED));
		            				BackMsg.send(worker);
	            				}
	            				else
	            				{
	            					ZMsg ToOuterNode=new ZMsg();
	            					ToOuterNode.addLast(Integer.toString(DATABUS.GET_FROM_CHANNEL));
	            					ToOuterNode.addLast(key);
	            					ZMsg RecvMsg=RequestToOuterNode(ToOuterNode, Endpoint);
	            					appendMsg(BackMsg,RecvMsg).send(worker);
	            				}
	            			}
	            			break;
		            	}
	            	case DATABUS.GET_FROM_CHANNEL_BY_INDEX:
		            	{
	            			String key=RequestMsg.pop().toString();
	            			int index=Integer.parseInt(RequestMsg.pop().toString());
	            			ZMsg rtv=dataMap.getChannelData(key,index);
	            			if(rtv!=null){//数据在本地
	            				BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	            				appendMsg(BackMsg,rtv).send(worker);
	            			}
	            			else//数据不在本地
	            			{
	            				String Endpoint=dataMap.getChannelLocation(key);
	            				if(Endpoint==null)
	            				{
		            				BackMsg.addLast(Integer.toString(DATABUS.FAILED));
		            				BackMsg.send(worker);
	            				}
	            				else
	            				{
	            					ZMsg ToOuterNode=new ZMsg();
	            					ToOuterNode.addLast(Integer.toString(DATABUS.GET_FROM_CHANNEL_BY_INDEX));
	            					ToOuterNode.addLast(key);
	            					ToOuterNode.addLast(Integer.toString(index));
	            					ZMsg RecvMsg=RequestToOuterNode(ToOuterNode, Endpoint);
	            					appendMsg(BackMsg,RecvMsg).send(worker);
	            				}
	            			}
	            			break;
		            	}
	            	case DATABUS.FREE_CHANNEL:
		            	{
	            			String key=RequestMsg.pop().toString();
	            			dataMap.freeChannel(key);
	            			
	            			BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	    					BackMsg.send(worker);
	    					
	    					ZMsg ToKeyServer=new ZMsg();
	    					ToKeyServer.addLast(Integer.toString(DATABUS.FREE_CHANNEL));
	    					ToKeyServer.addLast(key);
	    					ZMsg UpdateResMsg=RequestToKeyServer(ToKeyServer);
	    					if(UpdateResMsg.pop().streq(Integer.toString(DATABUS.SUCCESSFULLY)))
	    						;
	    					else
	    						System.out.println("FREE_CHANNEL update to key server failed key="+key);
	    					break;
		            	}
	            	case DATABUS.GET_FILE_LOCATION:
		            	{
	            			String key=RequestMsg.pop().toString();
	            			
	            			if(dataMap.IsFileExist(key))
	            			{
	            				BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	            				BackMsg.addLast(DaemonMain.LocalEndpoint);
	            				BackMsg.send(worker);
	            			}
	            			else
	            			{
	            				String Endpoint=dataMap.getFileLocation(key);
	            				if(Endpoint!=null)
	            				{
	            					BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	            					BackMsg.addLast(Endpoint);
	            					BackMsg.send(worker);
	            				}
	            				else
	            				{
	            					BackMsg.addLast(Integer.toString(DATABUS.FAILED));
	            					BackMsg.send(worker);
	            				}
	            			}
	            			break;
		            	}
	            	case DATABUS.INSERT_A_FILE:
		            	{
	            			String key=RequestMsg.pop().toString();
	            			dataMap.storeLocalFile(key,RequestMsg.pop().toString());
	            			
	            			BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	    					BackMsg.send(worker);
	    					
	    					ZMsg ToKeyServer=new ZMsg();
	    					ToKeyServer.addLast(Integer.toString(DATABUS.INSERT_A_FILE));
	    					ToKeyServer.addLast(key);
	    					ToKeyServer.addLast(DaemonMain.LocalEndpoint);
	    					ZMsg UpdateResMsg=RequestToKeyServer(ToKeyServer);
	    					if(UpdateResMsg.pop().streq(Integer.toString(DATABUS.SUCCESSFULLY)))
	    						;
	    					else
	    						System.out.println("INSERT_A_FILE update to key server failed key="+key);
	    					break;
		            	}
	            	case DATABUS.GET_A_FILE:
		            	{
	            			String key=RequestMsg.pop().toString();
	            			long offset=Integer.parseInt(RequestMsg.pop().toString());
	            			long length=Integer.parseInt(RequestMsg.pop().toString());
	            			ZMsg rtv=dataMap.getFileDataWithOffset(key,offset,length);
	            			if(rtv!=null){//数据在本地
	            				BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	            				appendMsg(BackMsg,rtv).send(worker);
	            			}
	            			else//数据不在本地
	            			{
	            				String Endpoint=dataMap.getFileLocation(key);
	            				if(Endpoint==null)
	            				{
		            				BackMsg.addLast(Integer.toString(DATABUS.FAILED));
		            				BackMsg.send(worker);
	            				}
	            				else
	            				{
	            					ZMsg ToOuterNode=new ZMsg();
	            					ToOuterNode.addLast(Integer.toString(DATABUS.GET_A_FILE));
	            					ToOuterNode.addLast(key);
	            					ZMsg RecvMsg=RequestToOuterNode(ToOuterNode, Endpoint);
	            					appendMsg(BackMsg,RecvMsg).send(worker);
	            				}
	            			}
	            			break;
		            	}
	            	case DATABUS.FREE_A_FILE:
		            	{
	            			String key=RequestMsg.pop().toString();
	            			dataMap.freeFile(key);
	            			
	            			BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	    					BackMsg.send(worker);
	    					
	    					ZMsg ToKeyServer=new ZMsg();
	    					ToKeyServer.addLast(Integer.toString(DATABUS.FREE_A_FILE));
	    					ToKeyServer.addLast(key);
	    					ZMsg UpdateResMsg=RequestToKeyServer(ToKeyServer);
	    					if(UpdateResMsg.pop().streq(Integer.toString(DATABUS.SUCCESSFULLY)))
	    						;
	    					else
	    						System.out.println("FREE_A_FILE update to key server failed key="+key);
	    					break;
		            	}
	            	case DATABUS.DELETE_A_FILE:
		            	{
	            			String key=RequestMsg.pop().toString();
	            			dataMap.deleteFile(key);
	            			
	            			BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	    					BackMsg.send(worker);
	    					
	    					ZMsg ToKeyServer=new ZMsg();
	    					ToKeyServer.addLast(Integer.toString(DATABUS.DELETE_A_FILE));
	    					ToKeyServer.addLast(key);
	    					ZMsg UpdateResMsg=RequestToKeyServer(ToKeyServer);
	    					if(UpdateResMsg.pop().streq(Integer.toString(DATABUS.SUCCESSFULLY)))
	    						;
	    					else
	    						System.out.println("DELETE_A_FILE update to key server failed key="+key);
	    					break;
		            	}
	            	case DATABUS.GET_FILE_SIZE:
		            	{
	            			String key=RequestMsg.pop().toString();
	            			if(dataMap.IsFileExist(key))
	            			{
	            				long fileSize=dataMap.getFileSize(key);
		            			
		            			BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
		    					BackMsg.addLast(Long.toString(fileSize));
		            			BackMsg.send(worker);
	            			}
	            			else//数据不在本地
	            			{
	            				String Endpoint=dataMap.getFileLocation(key);
	            				if(Endpoint==null)
	            				{
		            				BackMsg.addLast(Integer.toString(DATABUS.FAILED));
		            				BackMsg.send(worker);
	            				}
	            				else
	            				{
	            					ZMsg ToOuterNode=new ZMsg();
	            					ToOuterNode.addLast(Integer.toString(DATABUS.GET_FILE_SIZE));
	            					ToOuterNode.addLast(key);
	            					ZMsg RecvMsg=RequestToOuterNode(ToOuterNode, Endpoint);
	            					appendMsg(BackMsg,RecvMsg).send(worker);
	            				}
	            			}
	            			break;
	            		}
	            	default:
	            		BackMsg.addLast(Integer.toString(DATABUS.FAILED));
        				BackMsg.send(worker);
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
};