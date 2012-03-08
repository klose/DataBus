package com.longyi.databus.exception;

public class DSFileIndexOutOfBoundException extends Exception{
	/**
	 * 
	 */
	private static final long serialVersionUID = 5168350179763553501L;
	public DSFileIndexOutOfBoundException()
	{
		super();
	}
	public DSFileIndexOutOfBoundException(String msg)
	{
		super(msg);
	}
	public DSFileIndexOutOfBoundException(String msg,Throwable cause)
	{
		super(msg,cause);
	}
	public DSFileIndexOutOfBoundException(Throwable cause)
	{
		super(cause);
	}
}
