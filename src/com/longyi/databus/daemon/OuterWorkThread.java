package com.longyi.databus.daemon;

import java.util.Iterator;
import java.util.List;

import org.zeromq.ZFrame;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;
import org.zeromq.ZMsg;
import org.zeromq.ZMQ.Context;

import com.longyi.databus.define.DATABUS;
import com.longyi.databus.define.ValueObject;

public class OuterWorkThread extends Thread{
	private Context context;
    private final ZMQ.Poller poller;
	private final ZMQ.Socket worker;
	private final DataMap dataMap;
    public OuterWorkThread(Context _context)
	{
		context=_context;
		this.poller = context.poller(1);
        worker=context.socket(ZMQ.DEALER);
		worker.connect(DATABUS.OUTBACKEND);
		this.poller.register(worker, ZMQ.Poller.POLLIN);
		dataMap=new DataMap();
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
	            	case DATABUS.JOB_GET_KEY_BYTE:
	            	{
	            		String jobId=RequestMsg.pop().toString();
	            		String partionId=RequestMsg.pop().toString();
	            		String key=RequestMsg.pop().toString();
	            		JobDataMap _tmpJobDataMap=DataMapForJob.JobDataMapFactory(jobId,DATABUS.JOB_VALUE_BYTE);
	            		BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	            		List<byte[]> rtvValue=_tmpJobDataMap.getkeyByte(partionId, key);
	            		for(byte[] tmp:rtvValue)
	            		{
	            			BackMsg.addFirst(tmp);
	            		}
	            		BackMsg.send(worker);
	            	}
	            	case DATABUS.JOB_GET_KEY_OBJECT:
	            	{
	            		String jobId=RequestMsg.pop().toString();
	            		String partionId=RequestMsg.pop().toString();
	            		String key=RequestMsg.pop().toString();
	            		
	            		JobDataMap _tmpJobDataMap=DataMapForJob.JobDataMapFactory(jobId,DATABUS.JOB_VALUE_OBJECT);
	            		BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	            		List<ValueObject> rtvValue=_tmpJobDataMap.getkeyObject(partionId, key);
	            		for(ValueObject tmp:rtvValue)
	            		{
	            			BackMsg.addFirst(tmp.getBytes());
	            		}
	            		BackMsg.send(worker);
	            	}
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
		            			BackMsg.addLast(Integer.toString(DATABUS.FAILED));
		            			BackMsg.send(worker);
	            			}
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
		            			BackMsg.addLast(Integer.toString(DATABUS.FAILED));
		            			BackMsg.send(worker);
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
	            				BackMsg.addLast(Integer.toString(DATABUS.FAILED));
		            			BackMsg.send(worker);
	            			}
	            			break;
	            		}
	            	case DATABUS.SET_A_FILE:
		            	{
		            		String key=RequestMsg.pop().toString();
	            			long offset=Integer.parseInt(RequestMsg.pop().toString());
	            			long length=Integer.parseInt(RequestMsg.pop().toString());
	            			ZMsg data=RequestMsg;
	            			int rtv=dataMap.storeOtherNodeFile(key, data, offset, length);
	            			if(rtv==1)
	            			{
	            				BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	            				BackMsg.send(worker);
	            			}
	            			else
	            			{
	            				BackMsg.addLast(Integer.toString(DATABUS.FAILED));
	            				BackMsg.send(worker);
	            			}
	            			break;
		            	}
	            	case DATABUS.GET_A_FILE:
		            	{
	            			String key=RequestMsg.pop().toString();
	            			int offset=Integer.parseInt(RequestMsg.pop().toString());
	            			int length=Integer.parseInt(RequestMsg.pop().toString());
	            			ZMsg rtv=dataMap.getFileDataWithOffset(key,offset,length);
	            			if(rtv!=null){//数据在本地
	            				BackMsg.addLast(Integer.toString(DATABUS.SUCCESSFULLY));
	            				appendMsg(BackMsg,rtv).send(worker);
	            			}
	            			else//数据不在本地
	            			{
		            			BackMsg.addLast(Integer.toString(DATABUS.FAILED));
		            			BackMsg.send(worker);
	            			}
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
		            			BackMsg.addLast(Integer.toString(DATABUS.FAILED));
		            			BackMsg.send(worker);
	            			}
	            			break;
		            	}
	            	case DATABUS.GET_MESSAGE_LOCATION:
	            	case DATABUS.SEND_MESSAGE:
	            	case DATABUS.FREE_MESSAGE:
	            	case DATABUS.GET_CHANNEL_LOCATION:
	            	case DATABUS.SEND_TO_CHANNEL:
	            	case DATABUS.FREE_CHANNEL:
	            	case DATABUS.GET_FILE_LOCATION:
	            	case DATABUS.INSERT_A_FILE:
	            	case DATABUS.FREE_A_FILE:
	            	case DATABUS.DELETE_A_FILE:
	            	default:
	            		BackMsg.addLast(Integer.toString(DATABUS.FAILED));
            			BackMsg.send(worker);
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
};
