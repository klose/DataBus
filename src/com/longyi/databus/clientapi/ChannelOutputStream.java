package com.longyi.databus.clientapi;
import com.longyi.databus.define.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.zeromq.*;
public class ChannelOutputStream {
	private String ChannelName=null;
	private byte[] DataBuffer=null;
	private List<byte[]> DataBufferMany=null;
	private int InputPtr;
	private DataBusAPI dataBusAPI=null;
	private int DataBufferManyContentSize=0;
	
	public ChannelOutputStream(String _ChannelName)
	{
		InputPtr=0;
		DataBufferManyContentSize=0;
		DataBuffer=new byte[DATABUS.BUFFER_SIZE];
		DataBufferMany=new ArrayList<byte[]>();
		ChannelName=_ChannelName;
		dataBusAPI=new DataBusAPI();
	};
	/*
	public void flush()
	{
		byte[] SendArray=new byte[InputPtr];
		System.arraycopy(DataBuffer, 0, SendArray, 0, InputPtr);
		dataBusAPI.sendDataToChannel(ChannelName,SendArray);
		DataBuffer=new byte[DATABUS.BUFFER_SIZE];
		InputPtr=0;
	};
	*/
	public int write(byte[] data) {
		DataBufferMany.add(data);
		DataBufferManyContentSize+=data.length;
		if(DataBufferManyContentSize>DATABUS.BUFFER_SIZE)
		{
			if(dataBusAPI.sendDataToChannelMany(ChannelName, DataBufferMany)==1)
			{
				DataBufferManyContentSize=0;
				DataBufferMany=new ArrayList<byte[]>();
				return 1;
			}
			else
				return 0;
		}
		return 1;
	};
	public int flush()
	{
		if(dataBusAPI.sendDataToChannelMany(ChannelName, DataBufferMany)==1)
		{
			DataBufferManyContentSize=0;
			DataBufferMany=new ArrayList<byte[]>();
			return 1;
		}
		else
			return 0;
	}
	public int close()
	{
		if(dataBusAPI.sendDataToChannelMany(ChannelName, DataBufferMany)==1)
		{
			
			return 1;
		}
		else
			return 0;
	}
	/*
	private int write(byte[] data,int datasize)
	{
		int i=0;
		int tmpSendSize=0;
		for(i=0;i<datasize;i=i+tmpSendSize)
		{
			if(datasize-i>=DATABUS.BUFFER_SIZE-InputPtr)
			{
				tmpSendSize=DATABUS.BUFFER_SIZE-InputPtr;
				System.arraycopy(data, i, DataBuffer, InputPtr, DATABUS.BUFFER_SIZE-InputPtr);
				if(dataBusAPI.sendDataToChannel(ChannelName,DataBuffer)<0)
					return -1;
				DataBuffer=new byte[DATABUS.BUFFER_SIZE];
				InputPtr=0;
			}
			else
			{
				tmpSendSize=datasize-i;
				System.arraycopy(data, i, DataBuffer, InputPtr, datasize-i);
				InputPtr=InputPtr+datasize-i;
			}
		}
		return 1;
	}
	*/
	/*
	private int write(byte[] data,int datasize)
	{
		DataBufferMany.add(data);
		return 1;
	}
	*/
	/*
	public int close()
	{
		byte[] SendArray=new byte[InputPtr];
		System.arraycopy(DataBuffer, 0, SendArray, 0, InputPtr);
		return dataBusAPI.sendDataToChannel(ChannelName,SendArray);
	}
	*/
};
