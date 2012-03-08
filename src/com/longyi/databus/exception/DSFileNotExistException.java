package com.longyi.databus.exception;

public class DSFileNotExistException extends Exception{
	/**
	 * 
	 */
	private static final long serialVersionUID = 5168350179763553501L;
	public DSFileNotExistException()
	{
		super();
	}
	public DSFileNotExistException(String msg)
	{
		super(msg);
	}
	public DSFileNotExistException(String msg,Throwable cause)
	{
		super(msg,cause);
	}
	public DSFileNotExistException(Throwable cause)
	{
		super(cause);
	}
}
