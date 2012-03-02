package com.longyi.databus.clientapi;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.zeromq.*;
public class ChannelInputStream {
	private class ThreadReader extends Thread
	{
		private String ChannelName=null;
		private DataBusAPI dataBusAPI=null;
		private int MsgIndex=0;
		ThreadReader(String _ChannelName)
		{
			MsgIndex=0;
			ChannelName=_ChannelName;
			dataBusAPI=new DataBusAPI();
		}
		@SuppressWarnings("deprecation")
		public void run()
		{
			while(true)
			{
				byte[] channelData=dataBusAPI.getDataFromChannel(ChannelName, MsgIndex);
				if(channelData!=null)
				{
					MsgIndex++;
					DataBufferArray.add(channelData);
				}
				else
				{
					Threadstate=0;
					this.suspend();
				}
			}
		}	
	}
	
	
	private final Queue<byte[]> DataBufferArray=new ConcurrentLinkedQueue<byte[]>();
	
	private byte[] rightBuffer;
	private String ChannelName;
	private int rightBufferPtr;
	private int rightBufferSize;
	private ThreadReader _dataTransferThread=null;
	private int Threadstate;
	ChannelInputStream(String _ChannelName)
	{
		rightBufferPtr=0;
		rightBufferSize=0;
		rightBuffer=null;
		ChannelName=_ChannelName;
		ThreadReader _dataTransferThread=new ThreadReader(ChannelName);
		_dataTransferThread.start();
		Threadstate=1;
		
	};
	public int close()
	{
		_dataTransferThread.stop();
		_dataTransferThread.stop();
		return 1;
	}
	public int skip(int datasize)
	{
		int dupdatasize=datasize;
		while(datasize>rightBufferSize-rightBufferPtr)
		{
			datasize=datasize-(rightBufferSize-rightBufferPtr);
			rightBuffer=DataBufferArray.poll();
			if(rightBuffer!=null)
			{
				rightBufferSize=rightBuffer.length;
				rightBufferPtr=0;
			}
			else
			{
				if(Threadstate==1)
				{
					while(Threadstate==1)
					{
						try {
							Thread.sleep(50);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							return dupdatasize-datasize;
						}
					}
					rightBuffer=DataBufferArray.poll();
					if(rightBuffer!=null)
					{
						rightBufferSize=rightBuffer.length;
						rightBufferPtr=0;
					}
					else
						return dupdatasize-datasize;
				}
				else
				{
					Threadstate=1;
					_dataTransferThread.resume();
					while(Threadstate==1)
					{
						try {
							Thread.sleep(50);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							return dupdatasize-datasize;
						}
					}
					rightBuffer=DataBufferArray.poll();
					if(rightBuffer!=null)
					{
						rightBufferSize=rightBuffer.length;
						rightBufferPtr=0;
					}
					else
						return dupdatasize-datasize;
				}
			}
		}
		rightBufferPtr=rightBufferPtr+datasize;
		return dupdatasize;
	}
	public byte[] read(int datasize)
	{
		byte[] rtv=new byte[datasize];
		
		int dupdatasize=datasize;
		while(datasize>rightBufferSize-rightBufferPtr)
		{
			datasize=datasize-(rightBufferSize-rightBufferPtr);
			rightBuffer=DataBufferArray.poll();
			if(rightBuffer!=null)
			{
				rightBufferSize=rightBuffer.length;
				rightBufferPtr=0;
			}
			else
			{
				if(Threadstate==1)
				{
					while(Threadstate==1)
					{
						try {
							Thread.sleep(50);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							//return dupdatasize-datasize;
							return null;
						}
					}
					rightBuffer=DataBufferArray.poll();
					if(rightBuffer!=null)
					{
						rightBufferSize=rightBuffer.length;
						rightBufferPtr=0;
					}
					else
						return null;
						//return dupdatasize-datasize;
				}
				else
				{
					Threadstate=1;
					_dataTransferThread.resume();
					while(Threadstate==1)
					{
						try {
							Thread.sleep(50);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							return null;
							//return dupdatasize-datasize;
						}
					}
					rightBuffer=DataBufferArray.poll();
					if(rightBuffer!=null)
					{
						rightBufferSize=rightBuffer.length;
						rightBufferPtr=0;
					}
					else
						return null;
						//return dupdatasize-datasize;
				}
			}
		}
		rightBufferPtr=rightBufferPtr+datasize;
		return null;
		//return dupdatasize;
	}
};



