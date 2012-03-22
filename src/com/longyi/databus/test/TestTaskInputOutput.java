package com.longyi.databus.test;

import java.util.ArrayList;
import java.util.List;

import com.longyi.databus.clientapi.TaskInput;
import com.longyi.databus.clientapi.TaskOutput;
import com.longyi.databus.daemon.DaemonMain;
import com.longyi.databus.daemon.PartionUpdateThread;
import com.longyi.databus.define.DATABUS;

public class TestTaskInputOutput {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Thread _daemon = new DaemonMain("192.168.1.123");
		_daemon.start();
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		String JobID="testJob";
		String PartionID1="Partion1";
		String PartionID2="Partion2";
		TaskOutput _output=new TaskOutput(JobID,DATABUS.JOB_VALUE_BYTE,null);
		List<byte[]> partion1LoveValue=new ArrayList<byte[]>();
		partion1LoveValue.add("I Love You".getBytes());
		partion1LoveValue.add("I Love You".getBytes());
		List<byte[]> partion1HateValue=new ArrayList<byte[]>();
		partion1HateValue.add("I hate You".getBytes());
		partion1HateValue.add("I hate You".getBytes());
		_output.putkeyByte(PartionID1, "Love", partion1LoveValue);
		_output.putkeyByte(PartionID1, "Hate", partion1HateValue);
		//_output.putkeyByte(PartionID1, "Love", partion1LoveValue);
		//_output.putkeyByte(PartionID1, "Hate", partion1HateValue);
		
		
		List<byte[]> partion2askValue=new ArrayList<byte[]>();
		partion1LoveValue.add("am i sex".getBytes());
		partion1LoveValue.add("am i sex".getBytes());
		List<byte[]> partion2ansValue=new ArrayList<byte[]>();
		partion1HateValue.add("so sex".getBytes());
		partion1HateValue.add("so sex".getBytes());
		_output.putkeyByte(PartionID2, "ask", partion2askValue);
		_output.putkeyByte(PartionID2, "ans", partion2ansValue);
		//_output.putkeyByte(PartionID2, "ask", partion2askValue);
		//_output.putkeyByte(PartionID2, "ans", partion2ansValue);
		
		TaskInput _input=new TaskInput(JobID,DATABUS.JOB_VALUE_BYTE);
		PartionUpdateThread _thread=_input.update(PartionID1);
		if(_thread==null)
			System.out.println("dddd");
		List<String> _keyList=_thread.getKeyList();
		for(int i=0;i<_keyList.size();i++)
		{
			System.out.println("Key= "+_keyList.get(i));
			String tmpkey=_thread.getPrepareKey();
			System.out.println("Parepare Key= "+tmpkey);
			System.out.println("value="+_input.getkeyByte(PartionID1, tmpkey).toString());
		}
		
	}

}
