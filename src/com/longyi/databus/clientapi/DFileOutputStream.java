package com.longyi.databus.clientapi;

import com.longyi.databus.define.DATABUS;

public class DFileOutputStream {
	private String FileName=null;
	private byte[] DataBuffer=null;
	private int InputPtr;
	private DataBusAPI dataBusAPI=null;
	private int FilePtr;
	private int FileSize;
	
	public DFileOutputStream(String _FileName)
	{
		FileSize=0;
		InputPtr=0;
		FilePtr=0;
		DataBuffer=new byte[DATABUS.BUFFER_SIZE];
		FileName=_FileName;
		dataBusAPI=new DataBusAPI();
		FileSize=dataBusAPI.getFileSize(_FileName);
		if(FileSize<0)
			System.out.println("File Not Exist "+FileName);
	};
	public void flush()
	{
		byte[] SendArray=new byte[InputPtr];
		System.arraycopy(DataBuffer, 0, SendArray, 0, InputPtr);
		dataBusAPI.setFileData(FileName, SendArray, FilePtr, SendArray.length);
		FilePtr=FilePtr+SendArray.length;
		if(FilePtr>FileSize)
			FileSize=FilePtr;
		System.out.println("FilePtr="+FilePtr);
		DataBuffer=new byte[DATABUS.BUFFER_SIZE];
		InputPtr=0;
	};
	public int write(byte[] data,int datasize)
	{
		int i=0;
		int tmpSendSize=0;
		for(i=0;i<datasize;i=i+tmpSendSize)
		{
			if(datasize-i>=DATABUS.BUFFER_SIZE-InputPtr)
			{
				tmpSendSize=DATABUS.BUFFER_SIZE-InputPtr;
				System.arraycopy(data, i, DataBuffer, InputPtr, DATABUS.BUFFER_SIZE-InputPtr);
				if(dataBusAPI.setFileData(FileName,DataBuffer,FilePtr,DATABUS.BUFFER_SIZE)<0)
					return -1;
				System.out.println("FilePtr="+FilePtr);
				FilePtr=FilePtr+DATABUS.BUFFER_SIZE;
				if(FilePtr>FileSize)
					FileSize=FilePtr;
				DataBuffer=new byte[DATABUS.BUFFER_SIZE];
				InputPtr=0;
				
			}
			else
			{
				//FilePtr=FilePtr+datasize-i;
				//if(FilePtr>FileSize)
				//	FileSize=FilePtr;
				tmpSendSize=datasize-i;
				System.arraycopy(data, i, DataBuffer, InputPtr, datasize-i);
				InputPtr=InputPtr+datasize-i;
			}
		}
		
		return 1;
	}
	public int DSfseek(int offset,String BASEPTR)
	{
		//byte[] SendArray=new byte[InputPtr];
		//System.arraycopy(DataBuffer, 0, SendArray, 0, InputPtr);
		//dataBusAPI.setFileData(FileName,SendArray,FilePtr,SendArray.length);
		flush();
		if(DATABUS.DSF_BEGIN.equals(BASEPTR))
		{
			if(offset<0)
				return -1;
			else
				FilePtr=offset;
			return 1;
		}
		else if(DATABUS.DSF_CURRENT.equals(BASEPTR))
		{
			if(FilePtr+offset<0)
				return -1;
			else
				FilePtr+=offset;
			return 1;
		}
		else if(DATABUS.DSF_END.equals(BASEPTR))
		{
			if(FileSize+offset<0)
				return -1;
			else
			{
				FilePtr=FileSize+offset;
				if(offset>0)
					FileSize=FileSize+offset;
				return 1;
			}
		}
		else
			return -1;
	}
	public int close()
	{
		byte[] SendArray=new byte[InputPtr];
		System.arraycopy(DataBuffer, 0, SendArray, 0, InputPtr);
		return dataBusAPI.setFileData(FileName,SendArray,FilePtr,SendArray.length);
	}
}
