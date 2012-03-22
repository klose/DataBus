package com.longyi.databus.define;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;



public class MSConfiguration {
	private static HashMap<String, String> configuration 
		= new HashMap<String, String>();
	private static int maxTasksOnEachSlave = 3;
	private static int jobStateTriggerThreadWaitTime = 50;
	private static int slaveExecutorManagerThreadWaitTime = 10;
	private static int jobStateWatcherThreadWaitTime = 500;
	private static String BinosTmpDir = "/tmp/binos-tmp";
	static {
		File MSproperties = new File("MS.properties");
		try {
			BufferedReader br = new BufferedReader(new FileReader(MSproperties));
			String tmp = "";
			while ((tmp = br.readLine()) != null) {
				if ((!tmp.trim().equals("")) && (tmp.indexOf("=") != -1)) {
					String a[] = tmp.trim().split("=");
					configuration.put(a[0], a[1]);
				}
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static int getMaxTasksOnEachSlave() {
		if(configuration.get("maxTasksOnEachSlave") == null) {
			return maxTasksOnEachSlave;
		}
		else {
			return Integer.parseInt(configuration.get("maxTasksOnEachSlave"));
		}
		
	}
	public static int getJobStateTriggerThreadWaitTime() {
		if(configuration.get("jobStateTriggerThreadWaitTime") == null) {
			return jobStateTriggerThreadWaitTime;
		}
		return Integer.parseInt(configuration.get("jobStateTriggerThreadWaitTime"));
	}
	
	public static int getSlaveExecutorManagerThreadWaitTime() {
		
		if(configuration.get("slaveExecutorManagerThreadWaitTime") == null) {
			return slaveExecutorManagerThreadWaitTime;
		}
		return Integer.parseInt(configuration.get("slaveExecutorManagerThreadWaitTime"));
	}
	
	public static int getJobStateWatcherThreadWaitTime() {
		if(configuration.get("jobStateWatcherThreadWaitTime") == null) {
			return jobStateWatcherThreadWaitTime;
		}
		return Integer.parseInt(configuration.get("jobStateWatcherThreadWaitTime"));
	}
	public static String getBinosTmpDir() {
		if (configuration.get("BinosTmpDir") == null) {
			File file = new File(BinosTmpDir);
			if (!file.exists()) {
				file.mkdirs();
			}
			configuration.put("BinosTmpDir", BinosTmpDir);
			return BinosTmpDir;
		}
		else {
			return configuration.get("BinosTmpDir");
		}
	}
	
	
	//just for testing
	public static void main(String[] args) {
		System.out.println(MSConfiguration.getMaxTasksOnEachSlave());
	}
}
