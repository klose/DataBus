package com.longyi.databus.clientapi;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
public class ChannelInputStream {
	private class ThreadReader extends Thread
	{
		public void run()
		{
			while(state)
			{
				byte[] channelData=dataBusAPI.getDataFromChannel(ChannelName, MsgIndex);
				
				if(channelData!=null)
				{
					//System.out.println("AutoMaticget Data");
					MsgIndex++;
					//System.out.println(MsgIndex+" data="+(new String(channelData)));
					DataBufferArray.add(channelData);
					QueueNumber++;
				}
				else
				{
					System.out.println("suspend");
					QueueNumber++;
					this.suspend();
					System.out.println("resume");
					try {
						Thread.sleep(50);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}	
	}
	
	
	private final Queue<byte[]> DataBufferArray=new ConcurrentLinkedQueue<byte[]>();
	
	private byte[] rightBuffer;
	private String ChannelName;
	private DataBusAPI dataBusAPI=null;
	private int MsgIndex=0;
	private int rightBufferPtr;
	private int rightBufferSize;
	private ThreadReader _dataTransferThread=null;
	private int QueueNumber=0;
	private boolean state=true;
	public ChannelInputStream(String _ChannelName)
	{
		rightBufferPtr=0;
		rightBufferSize=0;
		rightBuffer=null;
		ChannelName=_ChannelName;
		MsgIndex=0;
		dataBusAPI=new DataBusAPI();
		QueueNumber=0;
		_dataTransferThread=new ThreadReader();
		_dataTransferThread.start();		
	};
	public int close()
	{
		state=false;
		_dataTransferThread.resume();
		return 1;
	}
	public int skip(int datasize)
	{
		int dupdatasize=datasize;
		while(datasize>rightBufferSize-rightBufferPtr)
		{
			if(rightBuffer!=null)
				datasize=datasize-(rightBufferSize-rightBufferPtr);
			if(QueueNumber>0)
			{
				rightBuffer=DataBufferArray.poll();
				QueueNumber--;
				if(rightBuffer!=null)
				{
					rightBufferSize=rightBuffer.length;
					rightBufferPtr=0;
				}
			}
			else
			{
				_dataTransferThread.resume();
				while(QueueNumber==0)
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
				QueueNumber--;
				if(rightBuffer!=null)
				{
					rightBufferSize=rightBuffer.length;
					rightBufferPtr=0;
				}
				else
					return dupdatasize-datasize;
			}
			datasize=datasize-(rightBufferSize-rightBufferPtr);
		}
		rightBufferPtr=rightBufferPtr+datasize;
		return dupdatasize;
	}
	public int read(byte[] data,int datasize)
	{
		if(data==null)
			data=new byte[datasize];
		int dupdatasize=datasize;
		while(datasize>rightBufferSize-rightBufferPtr)
		{
			if(rightBuffer!=null){
				
				//byte[] printbuffer=new byte[rightBufferSize-rightBufferPtr];
				//System.arraycopy(rightBuffer, rightBufferPtr, printbuffer, 0,rightBufferSize-rightBufferPtr);
				//System.out.println("+++++++++"+new String(printbuffer));
				
				System.arraycopy(rightBuffer, rightBufferPtr, data, dupdatasize-datasize,rightBufferSize-rightBufferPtr);
				datasize=datasize-(rightBufferSize-rightBufferPtr);
			}
			if(QueueNumber>0)
			{
				rightBuffer=DataBufferArray.poll();
				QueueNumber--;
				if(rightBuffer!=null)
				{
					rightBufferSize=rightBuffer.length;
					rightBufferPtr=0;
				}
				//else
				//{
				//	return dupdatasize-datasize;
				//}
			}
			else
			{
				_dataTransferThread.resume();
				while(QueueNumber==0)
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
				QueueNumber--;
				if(rightBuffer!=null)
				{
					rightBufferSize=rightBuffer.length;
					rightBufferPtr=0;
				}
				else
				{
					System.out.println("May not correct");
					return dupdatasize-datasize;
				}
			}
		}
		//byte[] printbuffer=new byte[datasize];
		//System.arraycopy(rightBuffer, rightBufferPtr, printbuffer, 0,datasize);
		//System.out.println("+++++++++"+new String(printbuffer));
		
		System.arraycopy(rightBuffer, rightBufferPtr, data, dupdatasize-datasize,datasize);
		rightBufferPtr=rightBufferPtr+datasize;
		return dupdatasize;
	}
};



