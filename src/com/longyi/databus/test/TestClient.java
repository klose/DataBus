package com.longyi.databus.test;

import com.longyi.databus.clientapi.ChannelInputStream;
import com.longyi.databus.clientapi.ChannelOutputStream;
import com.longyi.databus.clientapi.DFileInputStream;
import com.longyi.databus.clientapi.DFileOutputStream;
import com.longyi.databus.clientapi.DataBusAPI;
import com.longyi.databus.daemon.DaemonMain;
import com.longyi.databus.define.DATABUS;
import com.longyi.databus.exception.DSFileIndexOutOfBoundException;
import com.longyi.databus.exception.DSFileNotExistException;

public class TestClient {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Thread _daemon = new DaemonMain("192.168.1.123");
		_daemon.start();
		_daemon.stop();
		/*
		String ChannelName="testchannel";
		ChannelInputStream input=new ChannelInputStream(ChannelName);
		ChannelOutputStream output=new ChannelOutputStream(ChannelName);
		DataBusAPI _dataBusAPI=new DataBusAPI();
		_dataBusAPI.insertAFile("hello", "/home/sophia-long/sample.txt");
		
		DFileOutputStream _output=new DFileOutputStream("hello");
		
		String Wdata1="ai si ni ma le";
		_output.write(Wdata1.getBytes(), 14);
		//_output.DSfseek(100, DATABUS.DSF_END);
		String Wdata2="xiang si ni ma le";
		_output.write(Wdata2.getBytes(), 17);
		_output.flush();
		
		DFileInputStream _input=null;
		try {
			_input = new DFileInputStream("hello");
		} catch (DSFileNotExistException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		byte[] ddd=new byte[_input.available()];
		try {
			_input.read(ddd, _input.available());
		} catch (DSFileIndexOutOfBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.print(new String(ddd));

		System.out.println("\nzzzzzzzzzzzzzzzzzzzzzzzz");
		String data1="love you very much";
		output.write(data1.getBytes(),data1.length());
		String data2="hate you very much";
		output.write(data2.getBytes(),data2.length());
		String data3="like you very much";
		output.write(data3.getBytes(),data3.length());
		output.flush();
		
		
		
		byte[] data=new byte[data1.length()];
		
		int rtvLength=input.read(data,data1.length());
		System.out.println(rtvLength);
		System.out.println("first return result="+(new String(data)));
		
		rtvLength=input.read(data,data1.length());
		System.out.println(rtvLength);
		System.out.println("second return result="+(new String(data)));
		
		rtvLength=input.read(data,data1.length());
		System.out.println(rtvLength);
		System.out.println("Third return result="+(new String(data)));
		
		rtvLength=input.read(data,data1.length());
		System.out.println(rtvLength);
		System.out.println("Fourth return result="+(new String(data)));
		
		String data4="ssss aaa ffff oooo";
		output.write(data4.getBytes(),data4.length());
		output.flush();

		rtvLength=input.read(data,data4.length());
		System.out.println(rtvLength);
		System.out.println("Fiveth return result="+(new String(data)));
		//input.close();
		
		rtvLength=input.read(data,data4.length());
		System.out.println(rtvLength);
		System.out.println("sixth return result="+(new String(data)));
		
		rtvLength=input.read(data,data4.length());
		System.out.println(rtvLength);
		System.out.println("seventh return result="+(new String(data)));
		*/
	}

}
