package com.longyi.databus.daemon;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Iterator;
import java.util.UUID;
import java.util.Vector;

import org.zeromq.ZFrame;
import org.zeromq.ZMsg;

import com.longyi.databus.define.DATABUS;

public class DataMap {
	/*
	 * 所有在本地的数据信息
	 */
	private static HashMap<String,ZMsg> MessageMap=new HashMap<String,ZMsg>();
	private static HashMap<String,String> MessageSwapFileMap=new HashMap<String,String>();
	private static long MessageDataSize=0;
	private static HashMap<String,Vector<ZMsg> > ChannelMap=new HashMap<String,Vector<ZMsg> >();//key Identity, second string file path
	private static HashMap<String,int []> ChannelSwapFileMap=new HashMap<String,int []>();
	private static long ChannelDataSize=0;
	private static HashMap<String,String> FileMap=new HashMap<String,String>();//key Identity, second string file path
	/*
	 * 元数据location信息
	 */
	private static HashMap<String,String> MessageLocationMap=new HashMap<String,String>();
	private static HashMap<String,String> ChannelLocationMap=new HashMap<String,String>();
	private static HashMap<String,String> FileLocationMap=new HashMap<String,String>();
	DataMap()
	{
	};
	//元数据获取与更新
	public boolean creatMessageLocationMap(String data)
	{
		//MessageLocationMap=new HashMap<String,String>(data);
		String realdata=data.substring(1, data.length()-1);
		String[] keyvalue=realdata.split(", ");
		for(int i=0;i<keyvalue.length;i++)
		{
			int indexofequall=keyvalue[i].indexOf('=');
			if(indexofequall==-1)
				break;
			MessageLocationMap.put(keyvalue[i].substring(0, indexofequall),keyvalue[i].substring(indexofequall+1));
		}
		return true;
	}
	public boolean creatChannelLocationMap(String data)
	{
		String realdata=data.substring(1, data.length()-1);
		String[] keyvalue=realdata.split(", ");
		for(int i=0;i<keyvalue.length;i++)
		{
			int indexofequall=keyvalue[i].indexOf('=');
			if(indexofequall==-1)
				break;
			ChannelLocationMap.put(keyvalue[i].substring(0, indexofequall),keyvalue[i].substring(indexofequall+1));
		}
		return true;
	}
	public boolean creatFileLocationMap(String data)
	{
		String realdata=data.substring(1, data.length()-1);
		String[] keyvalue=realdata.split(", ");
		for(int i=0;i<keyvalue.length;i++)
		{
			int indexofequall=keyvalue[i].indexOf('=');
			if(indexofequall==-1)
				break;
			FileLocationMap.put(keyvalue[i].substring(0, indexofequall),keyvalue[i].substring(indexofequall+1));
		}
		return true;
	}
	
	public boolean setMessageLocation(String key,String Endpoint)
	{
		boolean rtv=MessageLocationMap.containsValue(key);
		MessageLocationMap.put(key,Endpoint);
		return rtv;
	}
	
	public boolean setChannelLocation(String key,String Endpoint)
	{
		boolean rtv=ChannelLocationMap.containsValue(key);
		ChannelLocationMap.put(key,Endpoint);
		return rtv;
	}
	
	public boolean setFileLocation(String key,String Endpoint)
	{
		boolean rtv=FileLocationMap.containsValue(key);
		FileLocationMap.put(key,Endpoint);
		return rtv;
	}
	
	public String getMessageLocation(String key)
	{
		return MessageLocationMap.get(key);
	}
	public String getChannelLocation(String key)
	{
		return ChannelLocationMap.get(key);
	}
	public String getFileLocation(String key)
	{
		return FileLocationMap.get(key);
	}
	
	public void freeMessageLocation(String key)
	{
		MessageLocationMap.remove(key);
	}
	public void freeChannelLocation(String key)
	{
		ChannelLocationMap.remove(key);
	}
	public void freeFileLocation(String key)
	{
		FileLocationMap.remove(key);
	}
	//本地数据操作与更新
	//本地数据的操作
	public boolean IsMessageExist(String key)
	{
		if(MessageMap.containsKey(key))
			return true;
		else
			return MessageSwapFileMap.containsKey(key);
	};
	public boolean IsChannelExist(String key)
	{
		if(ChannelMap.containsKey(key))
			return true;
		else
			return ChannelSwapFileMap.containsKey(key);
	}
	public boolean IsFileExist(String key)
	{
		return FileMap.containsKey(key);
	}
	//All store message are single frame
	/*
	 * Message 操作
	 */
	public ZMsg getMessage(String key)
	{
		ZMsg rtv=MessageMap.get(key);
		if(rtv==null)
		{
			String MessageLocation=MessageSwapFileMap.get(key);
			if(MessageLocation==null)
				return null;
			else
			{
				try {
					FileInputStream in=new FileInputStream(MessageLocation);
					DataInputStream DataIn=new DataInputStream(in);
					rtv=ZMsg.load(DataIn);
					return rtv;
				} catch (FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
			}
		}
		else
			return rtv;//.duplicate();
		
	};
	private int storeMessageToDisk(String key,ZMsg data,long datasize)
	{
		try {
			UUID fileid=UUID.randomUUID();
			String Filepath=DATABUS.FILE_DATA_PATH+"/Message/"+fileid.toString();
			System.out.println(Filepath);
			File file=new File(Filepath);
			if(!file.exists())
				try {
					file.createNewFile();
				} catch (IOException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
					System.out.println("Can not create file");
					return -1;
				}
			FileOutputStream Out=new FileOutputStream(file);
			DataOutputStream DataOut=new DataOutputStream(Out);
			try {
				ZMsg.save(data, DataOut);
				Out.close();
				MessageSwapFileMap.put(key, Filepath);
				return 1;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				try {
					Out.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					return -1;
				}
				return -1;
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	};
	public int storeMessage(String key,ZMsg data,long datasize)
	{
		ZMsg rtv=MessageMap.get(key);
		String MessageLocation=MessageSwapFileMap.get(key);
		if(rtv==null&&MessageLocation==null)
		{
			if(datasize<=DATABUS.SWAP_GAP)
			{
				MessageDataSize+=datasize;
				MessageMap.put(key, data);
				return 1;
			}
			else
			{
				return storeMessageToDisk(key,data,datasize);
			}
		}
		else if(rtv!=null)//同样的key第二次插入时，自动替换以前的数据
		{
			MessageDataSize=MessageDataSize-rtv.contentSize();
			if(datasize<=DATABUS.SWAP_GAP)
			{
				MessageDataSize+=datasize;
				MessageMap.put(key, data);
				return 1;
			}
			else
			{
				MessageMap.remove(key);
				return storeMessageToDisk(key,data,datasize);
			}
		}
		else
		{
			return storeMessageToDisk(key,data,datasize);
		}
	};
	public int freeMessage(String key)
	{
		ZMsg rtv=MessageMap.get(key);
		if(rtv==null)
		{
			String MessageLocation=MessageSwapFileMap.get(key);
			if(MessageLocation==null)
				return 0;
			else
			{
				MessageSwapFileMap.remove(key);
				//删除文件
				File file=new File(MessageLocation);
				if(file.exists())
					file.delete();
				return 1;
			}
		}
		else
		{
			MessageDataSize=MessageDataSize-rtv.contentSize();
			MessageMap.get(key).destroy();
			MessageMap.remove(key);
			return 1;
		}
	}
	
	
	public ZMsg getChannelData(String key,int index)
	{
		ZMsg rtv=null;
		Vector<ZMsg> MsgList=ChannelMap.get(key);
		int[] offsetArray=ChannelSwapFileMap.get(key);
		if(MsgList!=null)
		{
			if(MsgList.size()>index)
			{
				rtv=MsgList.get(index);//.duplicate();
				if(rtv!=null){
					return rtv;
				}
				else
				{
					if(offsetArray!=null)
					{
						try {
							String Filepath=DATABUS.FILE_DATA_PATH+"/Channel/"+key;
							File file=new File(Filepath);
							FileInputStream in=new FileInputStream(file);
							
							try {
								in.skip(offsetArray[index]);
								DataInputStream InStream=new DataInputStream(in);
								rtv=ZMsg.load(InStream);
								in.close();
								return rtv;
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
								return null;
							}
						} catch (FileNotFoundException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
							return null;
						}
					}
					return null;
				}
			}
			return null;
		}
		return null;
	};
	private int storeChannelMessageToDisk(String key,ZMsg data,long datasize,int index)
	{
		String Filepath=DATABUS.FILE_DATA_PATH+"/Channel/"+key;
		try {
			File file=new File(Filepath);
			if(!file.exists())
				try {
					file.createNewFile();
				} catch (IOException e2) {
					// TODO Auto-generated catch block
					e2.printStackTrace();
					return -1;
				}
			int offset=(int)file.length();
			FileOutputStream Out=new FileOutputStream(file,true);
			try {
			
				DataOutputStream DataOut=new DataOutputStream(Out);
				
				ZMsg.save(data, DataOut);
				int[] offsetArray=ChannelSwapFileMap.get(key);
				if(offsetArray==null)
				{
					offsetArray=new int[DATABUS.CHANNELDATANUBMER_MAX];
					offsetArray[index]=offset;
					ChannelSwapFileMap.put(key, offsetArray);
				}
				else
				{
					offsetArray[index]=offset;
				}
				Out.close();
				return 1;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				try {
					Out.close();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					return -1;
				}
				return -1;
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		}
	};
	public int storeChannelData(String key,ZMsg data,long datasize)
	{
		Vector<ZMsg> ZMsgArray=ChannelMap.get(key);
		int[] offsetArray=ChannelSwapFileMap.get(key);
		
		if(ZMsgArray==null&&offsetArray==null)
		{
			if(datasize<=DATABUS.SWAP_GAP)
			{
				ZMsgArray=new Vector<ZMsg>();
				ChannelDataSize+=datasize;
				ZMsgArray.add(data);
				ChannelMap.put(key, ZMsgArray);
				return 1;
			}
			else
			{
				ZMsgArray=new Vector<ZMsg>();
				ZMsgArray.add(null);
				ChannelMap.put(key, ZMsgArray);
				return storeChannelMessageToDisk(key,data,datasize,ZMsgArray.size()-1);
			}
		}
		else if(ZMsgArray!=null)
		{
			if(datasize<=DATABUS.SWAP_GAP)
			{
				ChannelDataSize+=datasize;
				ZMsgArray.add(data);
				return 1;
			}
			else
			{
				ZMsgArray.add(null);
				return storeChannelMessageToDisk(key,data,datasize,ZMsgArray.size()-1);
			}
		}
		else
		{
			ZMsgArray=new Vector<ZMsg>();
			ZMsgArray.add(null);
			ChannelMap.put(key, ZMsgArray);
			return storeChannelMessageToDisk(key,data,datasize,ZMsgArray.size()-1);
		}
	};
	//ZMsg list with number of frame;
	private ZMsg appendMsg(ZMsg Target,ZMsg Src)
    {
    	Iterator<ZFrame> i=Src.iterator();
    	while(i.hasNext()) {
            ZFrame f = i.next();
            Target.addLast(f);
    	}
    	return Target;
    };
	public ZMsg getAllChannelData(String key)
	{
		ZMsg rtv=new ZMsg();
		Vector<ZMsg> MsgList=ChannelMap.get(key);
		if(MsgList==null)
			return null;
		int size=MsgList.size();
		for(int i=0;i<size;i++)
		{
			ZMsg tmp=getChannelData(key,i);
			if(tmp!=null)
			{
				rtv=appendMsg(rtv,tmp);
			}
		}
		return rtv;
	}
	//ZMsg list with number of frame;
	public int freeChannel(String key)
	{
		Vector<ZMsg> MsgList=ChannelMap.get(key);
		if(MsgList!=null)
		{
			int MsgListSize=MsgList.size();
			for(int i=0;i<MsgListSize;i++)
				if(MsgList.get(i)!=null){
					ChannelDataSize=ChannelDataSize-MsgList.get(i).contentSize();
					MsgList.get(i).destroy();
				}
			ChannelMap.remove(key);
			int[] offsetArray=ChannelSwapFileMap.get(key);
			if(offsetArray!=null)
			{
				ChannelSwapFileMap.remove(key);
				String Filepath=DATABUS.FILE_DATA_PATH+"/Channel/"+key;
				File file=new File(Filepath);
				if(file.exists())
					file.delete();
			}
			return 1;
		}
		return 0;
	}
	
	public ZMsg getFileData(String key)
	{
		String Location=FileMap.get(key);
		if(Location!=null)
		{
			try {
				FileInputStream in=new FileInputStream(key);
				byte[] data=null;
				try {
					data = new byte[in.available()];
					in.read(data);
					ZMsg rtv=new ZMsg();
					rtv.addLast(data);
					return rtv;
				}
				catch(IOException e)
				{
					e.printStackTrace();
					return null;
				}
			}
			catch (FileNotFoundException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		return null;
	}
	//ZMsg list with number of frame;
	public ZMsg getFileDataWithOffset(String key,int offset,int length)
	{
		String Location=FileMap.get(key);
		if(Location!=null)
		{
			try {
				File file=new File(Location);
				if(!file.exists())
					try {
						file.createNewFile();
					} catch (IOException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
						return null;
					}
				FileInputStream in=new FileInputStream(file);
				
				byte[] data=null;
				try {
					data = new byte[length];
					in.skip(offset);
					in.read(data, 0, length);
					in.close();
					ZMsg rtv=new ZMsg();
					rtv.addLast(data);
					return rtv;
				}
				catch(IOException e)
				{
					e.printStackTrace();
					return null;
				}
			}
			catch (FileNotFoundException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
		return null;
	}
	
	public int storeLocalFile(String key,String filepath)//direct path
	{
		File file=new File(filepath);
		if(file.exists())
		{
			FileMap.put(key, filepath);
			return 1;
		}
		return 0;
	};
	//storeOtherNodeFile ZMsg data single frame
	public int storeOtherNodeFile(String key,ZMsg data,long offset,long length)
	{
		String filepath=FileMap.get(key);
		if(filepath!=null)
		{
			try{
				File file=new File(filepath);
				if(!file.exists())
					file.createNewFile();
				RandomAccessFile raf = new RandomAccessFile(file, "rw");
				raf.seek(offset);
				raf.write(data.pop().getData(),0, (int)length);
				raf.close();
			}catch(FileNotFoundException e)
			{
				e.printStackTrace();
				return -1;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return -1;
			}
			return 1;
		}
		else
			return -1;
	};
	public int freeFile(String key)
	{
		String Path=FileMap.get(key);
		if(Path!=null)
		{
			FileMap.remove(key);
			return 1;
		}
		else
		{
			return 0;
		}
	}
	public int deleteFile(String key)
	{
		String Path=FileMap.get(key);
		if(Path!=null)
		{
			FileMap.remove(key);
			File file=new File(Path);
			if(file.exists()){
				file.delete();
				return 1;
			}
			else
				return 0;
		}
		else
		{
			return 0;
		}
	}
	public long getFileSize(String key)
	{
		String Location=FileMap.get(key);
		if(Location!=null)
		{
			try {
				FileInputStream in=new FileInputStream(Location);
				try {
					return in.available();
				}
				catch(IOException e)
				{
					e.printStackTrace();
					return -1;
				}
			}
			catch (FileNotFoundException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
				return 0;
			}
		}
		return 0;
	}
};
