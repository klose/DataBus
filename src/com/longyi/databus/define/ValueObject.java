package com.longyi.databus.define;

public interface ValueObject {
	public byte[] getBytes();
	public ValueObject getObject(byte[] byteArray);
}
