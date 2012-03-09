package com.longyi.databus.clientapi;
import com.longyi.databus.define.*;
import com.longyi.databus.daemon.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.zeromq.*;
import org.zeromq.ZMQ.Context;
public class DataBusAPI {
	private String InProEndpoint=DATABUS.LOCAL_JAVA_DAEMON_ENDPOINT;
	private static Context context=DATABUS.context;
	private static final Queue<ZMQ.Socket> freeSocList=new ConcurrentLinkedQueue<ZMQ.Socket>();
	public DataBusAPI()
	{
		String LocalIpAddress = GetLocalIpAddress.getIpAddresses();
		this.InProEndpoint = DATABUS.LOCAL_JAVA_DAEMON_ENDPOINT + LocalIpAddress.substring(LocalIpAddress.length()-2) + ":" + DATABUS.ENDPOINT_PORT;
	};
	private ZMQ.Socket CreateNewSocToDaemon()
	{
		ZMQ.Socket tmpSoc=context.socket(ZMQ.REQ);
		tmpSoc.connect(InProEndpoint);
		return tmpSoc;
	};
	private ZMQ.Socket GetAFreeSoc()
	{
		if(!freeSocList.isEmpty())
		{
			return freeSocList.poll();
		}
		else
			return CreateNewSocToDaemon();
	};
	/*
	 * 如果返回值为空字符串，则不存在消息
	 * 如果返回值不为空
	 * 数据的物理地址在返回值中。正确返回值的形式为tcp://ip:port
	 * 用户自己提取。偶不太喜欢解析字符串，容易造成效率比较低的方法。
	 */
	private ZMsg SendRequest(ZMsg ReqeustMsg)
	{
		ZMQ.Socket SocketToLocalDataServer=GetAFreeSoc();
		ReqeustMsg.send(SocketToLocalDataServer);
		ZMsg recvMsg=ZMsg.recvMsg(SocketToLocalDataServer);
		freeSocList.offer(SocketToLocalDataServer);
		return recvMsg;
	};
	/*
	 * Request 标准First Frame=RequestName
	 * 			  Second Frame=data
	 * Reply   标准First Frame=成功与否辨识符号
	 *            如果成功 Second Frame=data
	 */
	public String getMessageLocation(String key)
	{
		
		ZMsg SendMsg = new ZMsg();
		SendMsg.addLast(Integer.toString(DATABUS.GET_MESSAGE_LOCATION));
		SendMsg.addLast(key);
		ZMsg Recvmsg=SendRequest(SendMsg);
		
		ZFrame FirstFrame=Recvmsg.pop();
		String BackString=new String(FirstFrame.getData());
		if(Integer.getInteger(BackString)==DATABUS.SUCCESSFULLY)
		{
			ZFrame SecondFrame=Recvmsg.pop();
			String rtvString=new String(SecondFrame.getData());
			return rtvString;
		}
		else
		{
			String s="";
			return s;
		}
	}
	public int sendMessage(String key,byte[] data)
	{
		ZMsg SendMsg = new ZMsg();
		SendMsg.addLast(Integer.toString(DATABUS.SEND_MESSAGE));
		SendMsg.addLast(key);
		SendMsg.addLast(data);
		ZMsg Recvmsg=SendRequest(SendMsg);
		ZFrame FirstFrame=Recvmsg.pop();
		String BackString =new String(FirstFrame.getData());
		if(Integer.parseInt(BackString)==DATABUS.SUCCESSFULLY)
			return 1;
		else if(Integer.parseInt(BackString)==DATABUS.FAILED)
			return 0;
		else
			return -1;
	};
	public int freeMessage(String key)
	{
		ZMsg SendMsg=new ZMsg();
		SendMsg.addLast(Integer.toString(DATABUS.FREE_MESSAGE));
		SendMsg.addLast(key);
		
		ZMsg Recvmsg=SendRequest(SendMsg);
		ZFrame FirstFrame=Recvmsg.pop();
		String BackString=new String(FirstFrame.getData());
		if(Integer.parseInt(BackString)==DATABUS.SUCCESSFULLY)
		{
			return 1;
		}
		else
		{
			return 0;
		}
	};
	/*
	 * 师兄这里java偶不太会，如果数据不存在应该在else中返回，不知道这个byte [] tmp=null;对不对而且返回程序该如何判断不太懂
	 * 这个很简单师兄自己搞搞
	 */
	public byte[] getMessage(String key)
	{
		ZMsg SendMsg=new ZMsg();
		SendMsg.addLast(Integer.toString(DATABUS.GET_MESSAGE));
		SendMsg.addLast(key);
		
		ZMsg Recvmsg=SendRequest(SendMsg);
		ZFrame FirstFrame=Recvmsg.pop();
		String BackString=new String(FirstFrame.getData());
		if(Integer.parseInt(BackString)==DATABUS.SUCCESSFULLY)
		{
			ZFrame SecondFrame=Recvmsg.pop();
			return SecondFrame.getData();
		}
		else
		{	
			return null;
		}
	};
	
	public int sendDataToChannel(String ChannelName, byte[] data)
	{
		ZMsg SendMsg = new ZMsg();
		SendMsg.addLast(Integer.toString(DATABUS.SEND_TO_CHANNEL));
		SendMsg.addLast(ChannelName);
		SendMsg.addLast(data);
		ZMsg Recvmsg=SendRequest(SendMsg);
		ZFrame FirstFrame=Recvmsg.pop();
		String BackString =new String(FirstFrame.getData());
		if(Integer.parseInt(BackString)==DATABUS.SUCCESSFULLY)
			return 1;
		else if(Integer.parseInt(BackString)==DATABUS.FAILED)
			return 0;
		else
			return -1;
	};
	public int sendDataToChannelMany(String ChannelName,List<byte[]> data)
	{
		ZMsg SendMsg = new ZMsg();
		SendMsg.addLast(Integer.toString(DATABUS.SEND_TO_CHANNEL));
		SendMsg.addLast(ChannelName);
		for (byte[] tmp: data) {
			SendMsg.addLast(tmp);
		}
		ZMsg Recvmsg=SendRequest(SendMsg);
		String BackString=Recvmsg.pop().toString();
		if(Integer.parseInt(BackString)==DATABUS.SUCCESSFULLY)
			return 1;
		else if(Integer.parseInt(BackString)==DATABUS.FAILED)
			return 0;
		else
			return -1;
	}
	public List<byte[]> getDataFromChannel(String ChanneName,int index)
	{
		ZMsg SendMsg=new ZMsg();
		SendMsg.addLast(Integer.toString(DATABUS.GET_FROM_CHANNEL_BY_INDEX));
		SendMsg.addLast(ChanneName);
		SendMsg.addLast(Integer.toString(index));
		
		ZMsg Recvmsg=SendRequest(SendMsg);
		ZFrame FirstFrame=Recvmsg.pop();
		String BackString=new String(FirstFrame.getData());
		if(Integer.parseInt(BackString)==DATABUS.SUCCESSFULLY)
		{
			List<byte[]> rtv=new ArrayList<byte[]>();
			int NumberofFrame=Recvmsg.size();
			for(int i=0;i<NumberofFrame;i++)
			{
				rtv.add(Recvmsg.pop().getData());
			}
			return rtv;
		}
		else
		{
			List<byte[]> tmp=new ArrayList<byte[]>();
			return tmp;
		}
	};
	public List<byte[]> getALLDataFromChannel(String ChanneName)
	{
		ZMsg SendMsg=new ZMsg();
		SendMsg.addLast(Integer.toString(DATABUS.GET_FROM_CHANNEL));
		SendMsg.addLast(ChanneName);

		ZMsg Recvmsg=SendRequest(SendMsg);
		ZFrame FirstFrame=Recvmsg.pop();
		String BackString=new String(FirstFrame.getData());
		if(Integer.parseInt(BackString)==DATABUS.SUCCESSFULLY)
		{
			List<byte[]> rtv=new ArrayList<byte[]>();
			int NumberofFrame=Recvmsg.size();
			for(int i=0;i<NumberofFrame;i++)
			{
				rtv.add(Recvmsg.pop().getData());
			}
			return rtv;
		}
		else
		{
			List<byte[]> tmp=new ArrayList<byte[]>();
			return tmp;
		}
	};
	public int freeChannel(String ChannelName)
	{
		ZMsg SendMsg = new ZMsg();
		SendMsg.addLast(Integer.toString(DATABUS.FREE_CHANNEL));
		SendMsg.addLast(ChannelName);
		ZMsg Recvmsg=SendRequest(SendMsg);
		ZFrame FirstFrame=Recvmsg.pop();
		String BackString =new String(FirstFrame.getData());
		if(Integer.parseInt(BackString)==DATABUS.SUCCESSFULLY)
			return 1;
		else if(Integer.parseInt(BackString)==DATABUS.FAILED)
			return 0;
		else
			return -1;
	};
	public int insertAFile(String key,String FilePath)
	{
		ZMsg SendMsg = new ZMsg();
		SendMsg.addLast(Integer.toString(DATABUS.INSERT_A_FILE));
		SendMsg.addLast(key);
		SendMsg.addLast(FilePath);
		ZMsg Recvmsg=SendRequest(SendMsg);
		ZFrame FirstFrame=Recvmsg.pop();
		String BackString =new String(FirstFrame.getData());
		if(Integer.parseInt(BackString)==DATABUS.SUCCESSFULLY)
			return 1;
		else if(Integer.parseInt(BackString)==DATABUS.FAILED)
			return 0;
		else
			return -1;
	};
	
	public String getFileLocation(String key)
	{
		ZMsg SendMsg = new ZMsg();
		SendMsg.addLast(Integer.toString(DATABUS.GET_FILE_LOCATION));
		SendMsg.addLast(key);
		ZMsg Recvmsg=SendRequest(SendMsg);
		ZFrame FirstFrame=Recvmsg.pop();
		String BackString =new String(FirstFrame.getData());
		if(Integer.parseInt(BackString)==DATABUS.SUCCESSFULLY)
		{
			ZFrame LocationFrame=Recvmsg.pop();
			return LocationFrame.toString();
		}
		else
			return "";
	};
	
	public byte[] getFileData(String key,int begin,int length)
	{
		ZMsg SendMsg = new ZMsg();
		
		SendMsg.addLast(Integer.toString(DATABUS.GET_A_FILE));
		SendMsg.addLast(key);
		SendMsg.addLast(Integer.toString(begin));
		SendMsg.addLast(Integer.toString(length));
		
		ZMsg Recvmsg=SendRequest(SendMsg);
		ZFrame FirstFrame=Recvmsg.pop();
		String BackString =new String(FirstFrame.getData());
		if(Integer.parseInt(BackString)==DATABUS.SUCCESSFULLY)
		{
			ZFrame DataFrame=Recvmsg.pop();
			return DataFrame.getData();
		}
		else
			return null;
	}
	public int setFileData(String key,byte[] data,int begin,int length)
	{
		ZMsg SendMsg=new ZMsg();
		
		SendMsg.addLast(Integer.toString(DATABUS.SET_A_FILE));
		SendMsg.addLast(key);
		SendMsg.addLast(Integer.toString(begin));
		SendMsg.addLast(Integer.toString(length));
		SendMsg.addLast(data);
		ZMsg Recvmsg=SendRequest(SendMsg);
		ZFrame FirstFrame=Recvmsg.pop();
		String BackString =new String(FirstFrame.getData());
		if(Integer.parseInt(BackString)==DATABUS.SUCCESSFULLY)
		{
			return 1;
		}
		else
			return -1;
		
	}
	public int getFileSize(String key)
	{
		ZMsg SendMsg = new ZMsg();
		
		SendMsg.addLast(Integer.toString(DATABUS.GET_FILE_SIZE));
		SendMsg.addLast(key);
		
		ZMsg Recvmsg=SendRequest(SendMsg);
		ZFrame FirstFrame=Recvmsg.pop();
		String BackString =new String(FirstFrame.getData());
		if(Integer.parseInt(BackString)==DATABUS.SUCCESSFULLY)
		{
			return Integer.parseInt(Recvmsg.pop().toString());
		}
		else
			return -1;
	};
	public int freeAFile(String key)
	{
		ZMsg SendMsg = new ZMsg();
		
		SendMsg.addLast(Integer.toString(DATABUS.FREE_A_FILE));
		SendMsg.addLast(key);
		
		ZMsg Recvmsg=SendRequest(SendMsg);
		ZFrame FirstFrame=Recvmsg.pop();
		String BackString =new String(FirstFrame.getData());
		if(Integer.parseInt(BackString)==DATABUS.SUCCESSFULLY)
		{
			return 1;
		}
		else
			return -1;
	};
	public int deleteAFile(String key)
	{
		ZMsg SendMsg = new ZMsg();
		
		SendMsg.addLast(Integer.toString(DATABUS.DELETE_A_FILE));
		SendMsg.addLast(key);
		
		ZMsg Recvmsg=SendRequest(SendMsg);
		ZFrame FirstFrame=Recvmsg.pop();
		String BackString =new String(FirstFrame.getData());
		if(Integer.parseInt(BackString)==DATABUS.SUCCESSFULLY)
		{
			return 1;
		}
		else
			return -1;
	}
	/*
	public static void main(String[] args)
	{
		DataBusAPI _dataApi=new DataBusAPI();
		
		byte[] data=new byte[255*1024*1024];
		_dataApi.sendMessage("love", data);
		long start = System.currentTimeMillis();
		byte[] result=_dataApi.getMessage("love");
		
		{
			byte[] result1=_dataApi.getMessage("love");
			//System.out.println(result1.length);
			//System.out.println(new String(result1));
		}
		System.out.println("get twice 255m used" + (System.currentTimeMillis() - start) + "ms");
		String ChannelData1="ChannelData1";
		_dataApi.sendDataToChannel("heihei",ChannelData1.getBytes());
		String ChannelData2="ChannelData2";
		_dataApi.sendDataToChannel("heihei",ChannelData2.getBytes());
		//try {
		//	Thread.sleep(1000);
		//} catch (InterruptedException e) {
			// TODO Auto-generated catch block
		//	e.printStackTrace();
		//}
		System.out.println(new String(_dataApi.getDataFromChannel("heihei",0)));
		System.out.println(new String(_dataApi.getDataFromChannel("heihei",1)));
		System.out.println(new String(_dataApi.getALLDataFromChannel("heihei")));
		_dataApi.insertAFile("Client.cpp", "/tmp/Client.cpp");
		System.out.println(_dataApi.getFileSize("Client.cpp"));
		System.out.println(new String(_dataApi.getFileData("Client.cpp",0,_dataApi.getFileSize("Client.cpp"))));
		
		//
	}
	*/
};
