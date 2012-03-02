package com.longyi.databus.daemon;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

public class GetLocalIpAddress {
    public static String getIpAddresses() {
    	InetAddress ip = null;
	    try {
	    	Enumeration netInterfaces=NetworkInterface.getNetworkInterfaces();    	
		    while(netInterfaces.hasMoreElements())
		    {
		    	NetworkInterface ni=(NetworkInterface)netInterfaces.nextElement();
		    	Enumeration enumIp = ni.getInetAddresses();
		        while (enumIp.hasMoreElements()) {
			    	ip=(InetAddress) enumIp.nextElement();
			    	//System.out.println(ip.getHostAddress());
			    	//System.out.print(!ip.isSiteLocalAddress());
			    	//System.out.print(!ip.isLoopbackAddress());
			    	//System.out.println(ip.getHostAddress().indexOf(":")==-1);
			    	if(!ip.isLoopbackAddress()&&ip.getHostAddress().indexOf(":")==-1)
			    	{
			    		
			    		return ip.getHostAddress();
			    	}
			    	else
			    		ip=null;
		        }
		    }
	    } catch (SocketException ex) {
	    	ex.printStackTrace();
	    }
	    return null;
    }
}