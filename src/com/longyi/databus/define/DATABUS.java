package com.longyi.databus.define;

import org.zeromq.ZMQ;
import org.zeromq.ZMQ.Context;

public class DATABUS {
	public static final int GET_MESSAGE_LOCATION=0;
	public static final int SEND_MESSAGE=1;
	public static final int GET_MESSAGE=2;
	public static final int FREE_MESSAGE=3;
	
	public static final int GET_CHANNEL_LOCATION=10;
	public static final int SEND_TO_CHANNEL=11;
	public static final int GET_FROM_CHANNEL=12;
	public static final int GET_FROM_CHANNEL_BY_INDEX=14;
	public static final int FREE_CHANNEL=15;
	
	public static final int GET_FILE_LOCATION=21;
	public static final int INSERT_A_FILE=22;
	public static final int GET_A_FILE=23;
	public static final int SET_A_FILE=24;
	public static final int FREE_A_FILE=25;
	public static final int DELETE_A_FILE=26;
	public static final int GET_FILE_SIZE=27;
	
	public static final int PUBSYS=50;
	
	public static final int SUCCESSFULLY=100;
	public static final int FAILED=101;
	public static final int NOTEXIST=102;
	
	public static final int GET_ALL_KEY_INFO=1000;
	public static final Context context = ZMQ.context(10);

	
	public static final String LOCAL_CPP_DAEMON_ENDPOINT="tcp://127.0.0.1:56432";
	public static final String LOCAL_JAVA_DAEMON_ENDPOINT="ipc://";
	public static final int ENDPOINT_PORT = 56432;
	
	public static final long MAX_MEMORY_FOR_MESSAGE=1024*1024*1024*2;//2G
	public static final long MAX_MEMORY_FOR_CHANNEL=1024*1024*1024*2;//2G
	public static final long SWAP_GAP=256*1024*1024;
	public static final int BUFFER_SIZE=10*1024*1024;
	//Daemon使用
	public static final String OUTBACKEND="inproc://BackendEndpoint";
	public static final String INBACKEND="inproc://InBackEndpoint";
	//KeyServer使用
	public static final String KEYREQBACKEND="inproc://KeyReqBackEnd";
	public static final String KEYPUBBACKEND="inproc://KeyPubBackEnd";
	public static final String FILE_DATA_PATH="/tmp/";
	public static final int CHANNELDATANUBMER_MAX=1000;
	public static String DSF_BEGIN="DSF_BEGIN";
	public static String DSF_CURRENT="DSF_CURRENT";
	public static String DSF_END="DSF_END";
}
