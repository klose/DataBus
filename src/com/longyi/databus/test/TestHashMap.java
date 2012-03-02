package com.longyi.databus.test;
import java.util.HashMap;

public class TestHashMap {
	public static HashMap<String,String> testhash=new HashMap<String,String>();
	public static void main(String[] args)
	{
		testhash.put("nihao","wohenhao");
		testhash.put("aini","xiangni");
		testhash.put("henni","fanni");
		
		String data=testhash.toString();
		String realdata=data.substring(1, data.length()-1);
		String[] keyvalue=realdata.split(", ");

		for(int i=0;i<keyvalue.length;i++)
		{
			int indexofequall=keyvalue[i].indexOf('=');
			
			String key=keyvalue[i].substring(0, indexofequall);
			String value=keyvalue[i].substring(indexofequall+1);
			System.out.println(key);
			System.out.println(value);
		}
	}
}
