package com.longyi.databus.clientapi;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.longyi.databus.define.DATABUS;
import com.longyi.databus.exception.DSFileIndexOutOfBoundException;
import com.longyi.databus.exception.DSFileNotExistException;

public class DFileInputStream {
	private class ThreadReader extends Thread
	{
		public void run()
		{
			while(state)
			{
				if(QueueNumber>10)
					this.suspend();
				byte[] Filedata=null;
				if(FileSize-FilePtr>DATABUS.BUFFER_SIZE)
					Filedata=dataBusAPI.getFileData(FileName, FilePtr, DATABUS.BUFFER_SIZE);
				else
					Filedata=dataBusAPI.getFileData(FileName, FilePtr, FileSize-FilePtr);
				if(Filedata!=null)
				{
					//System.out.println("in run===="+(new String(Filedata)));
					FilePtr=FilePtr+Filedata.length;
					DataBufferArray.add(Filedata);
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
	private String FileName;
	private DataBusAPI dataBusAPI=null;
	
	private int FilePtr=0;
	private int FileSize=0;
	private int rightBufferPtr;
	private int rightBufferSize;
	private int CurrentFilePtr=0;
	private ThreadReader _dataTransferThread=null;
	
	private int QueueNumber=0;
	private boolean state=true;
	
	public DFileInputStream(String _FileName) throws DSFileNotExistException
	{
		rightBufferPtr=0;
		rightBufferSize=0;
		rightBuffer=null;
		FileName=_FileName;
		FilePtr=0;
		CurrentFilePtr=0;
		
		dataBusAPI=new DataBusAPI();
		FileSize=dataBusAPI.getFileSize(FileName);
		if(FileSize<0)
		{
			throw new DSFileNotExistException("Reqeust File Not Exist Exception FileKey="+FileName);
		}
		
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
	public int DSfseek(int offset,String BASEPTR) throws DSFileIndexOutOfBoundException
	{
		if(BASEPTR.equals(DATABUS.DSF_BEGIN))
		{
			if(offset<0||offset>FileSize){
				throw new DSFileIndexOutOfBoundException("Request fseek size out of file size!!! FileSize="+FileSize+" begin Ptr="+0+" length="+offset);
			}
			else
			{
				FilePtr=offset;
				CurrentFilePtr=offset;
				DataBufferArray.clear();
				QueueNumber=0;
				_dataTransferThread.resume();
				return 1;
			}
		}
		else if(BASEPTR.equals(DATABUS.DSF_END))
		{
			if(offset>0||FilePtr+offset<0){
				throw new DSFileIndexOutOfBoundException("Request fseek size out of file size!!! FileSize="+FileSize+" begin Ptr="+FileSize+" length="+offset);
			}
			else
			{
				FilePtr=FilePtr+offset;
				CurrentFilePtr=offset;
				DataBufferArray.clear();
				QueueNumber=0;
				_dataTransferThread.resume();
				return 1;
			}
		}
		else if(BASEPTR.equals(DATABUS.DSF_CURRENT))
		{
			if(CurrentFilePtr+offset<0||CurrentFilePtr+offset>FileSize)
			{
				throw new DSFileIndexOutOfBoundException("Request fseek size out of file size!!! FileSize="+FileSize+" begin Ptr="+CurrentFilePtr+" length="+offset);
			}
			else
			{
				if(offset>0)
				{
					while(offset>rightBufferSize-rightBufferPtr)
					{
						if(rightBuffer!=null)
							offset=offset-(rightBufferSize-rightBufferPtr);
						else
							break;
						if(QueueNumber>0)
						{
							rightBuffer=DataBufferArray.poll();
							QueueNumber--;
							if(rightBuffer!=null)
							{
								rightBufferSize=rightBuffer.length;
								rightBufferPtr=0;
							}
							if(offset<rightBufferSize-rightBufferPtr)
							{
								rightBufferPtr+=offset;
								return 1;
							}
						}
						else
							break;
					}
					CurrentFilePtr=CurrentFilePtr+offset;
					FilePtr=CurrentFilePtr;
					DataBufferArray.clear();
					QueueNumber=0;
					_dataTransferThread.resume();
					return 1;
				}
				else if(offset<0)
				{
					FilePtr=FilePtr+offset;
					FilePtr=CurrentFilePtr;
					DataBufferArray.clear();
					QueueNumber=0;
					_dataTransferThread.resume();
					return 1;
				}
				else
					return 1;
			}
		}
		else
			return -1;
	}
	public int fileSize()
	{
		return FileSize;
	}
	public int available()
	{
		return FileSize-CurrentFilePtr;
	}
	public int read(byte[] data,int datasize) throws DSFileIndexOutOfBoundException
	{
		if(datasize+CurrentFilePtr>FileSize)
			throw new DSFileIndexOutOfBoundException("Request size out of file size!!! FileSize="+FileSize+" begin Ptr="+CurrentFilePtr+" length="+datasize);
		if(data==null)
			data=new byte[datasize];
		int dupdatasize=datasize;
		
		while(datasize>rightBufferSize-rightBufferPtr)
		{
			System.out.println("0000000000000");
			if(QueueNumber<5)
				_dataTransferThread.resume();
			if(rightBuffer!=null){			
				System.arraycopy(rightBuffer, rightBufferPtr, data, dupdatasize-datasize,rightBufferSize-rightBufferPtr);
				datasize=datasize-(rightBufferSize-rightBufferPtr);
				System.out.println("11111111111");
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
				System.out.println("2222222222");
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
						CurrentFilePtr+=dupdatasize-datasize;
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
					CurrentFilePtr+=dupdatasize-datasize;
					return dupdatasize-datasize;
				}
				System.out.println("333333333333");
			}
			System.out.println("4444444444444");
		}
		
		System.arraycopy(rightBuffer, rightBufferPtr, data, dupdatasize-datasize,datasize);
		rightBufferPtr=rightBufferPtr+datasize;
		CurrentFilePtr+=dupdatasize;
		return dupdatasize;
	}
}
