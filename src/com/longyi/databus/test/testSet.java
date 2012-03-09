package com.longyi.databus.test;

import java.util.HashSet;
import java.util.Set;

public class testSet {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Set<String> KeyList=new HashSet<String>();
		KeyList.add("love");
		KeyList.add("hate");
		KeyList.add("love");
		KeyList.add("hate");
		KeyList.add("sex");
		KeyList.add("hhh");
		System.out.println(KeyList.toString());
	}

}
