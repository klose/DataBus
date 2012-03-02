package com.longyi.databus.clientapi;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.zeromq.*;

import com.longyi.databus.define.DATABUS;
public class ChannelOutputStream {
	private String ChannelName=null;
	private byte[] DataBuffer=null;
	private int InputPtr;
	private DataBusAPI dataBusAPI=null;
	
	ChannelOutputStream(String _ChannelName)
	{
		InputPtr=0;
		DataBuffer=new byte[DATABUS.BUFFER_SIZE];
		ChannelName=_ChannelName;
		dataBusAPI=new DataBusAPI();
	};
	public int write(byte[] data,int datasize)
	{
		int i=0;
		for(i=0;i<datasize;i=i+DATABUS.BUFFER_SIZE-InputPtr)
		{
			if(datasize-i>=DATABUS.BUFFER_SIZE-InputPtr)
			{	
				System.arraycopy(data, i, DataBuffer, InputPtr, DATABUS.BUFFER_SIZE-InputPtr);
				if(dataBusAPI.sendDataToChannel(ChannelName,DataBuffer)<0)
					return -1;
				DataBuffer=new byte[DATABUS.BUFFER_SIZE];
				InputPtr=0;
			}
			else
			{
				System.arraycopy(data, i, DataBuffer, InputPtr, datasize-i);
				InputPtr=InputPtr+datasize-i;
			}
		}
		return 1;
	}
	public int close()
	{
		byte[] SendArray=new byte[InputPtr];
		System.arraycopy(DataBuffer, 0, SendArray, 0, InputPtr);
		return dataBusAPI.sendDataToChannel(ChannelName,SendArray);
	}
};