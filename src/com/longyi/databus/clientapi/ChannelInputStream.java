package com.longyi.databus.clientapi;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
public class ChannelInputStream {
	private class ThreadReader extends Thread
	{
		public void run()
		{
			int MsgIndex=0;
			while(state)
			{
				List<byte[]> channelData=dataBusAPI.getDataFromChannel(ChannelName, MsgIndex);
				
				if(channelData!=null)
				{
					MsgIndex++;
					for(byte[] tmpByteArray:channelData)
						DataBufferArray.add(tmpByteArray);
				}
				else
				{
					DataBufferArray.add(null);
					this.suspend();
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
	
	private String ChannelName;
	private DataBusAPI dataBusAPI=null;
	private ThreadReader _dataTransferThread=null;
	private boolean state=true;
	public ChannelInputStream(String _ChannelName)
	{
		ChannelName=_ChannelName;
		dataBusAPI=new DataBusAPI();
		_dataTransferThread=new ThreadReader();
		_dataTransferThread.start();		
	};
	public int close()
	{
		state=false;
		_dataTransferThread.resume();
		return 1;
	}
	public byte[] read()
	{
		byte[] rtv=DataBufferArray.poll();
		if(rtv!=null)
			return rtv;
		else
		{
			_dataTransferThread.resume();
			while(DataBufferArray.size()!=0)
			{
				try {
					Thread.sleep(20);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			rtv=DataBufferArray.poll();
			return rtv;
		}
	}
};



