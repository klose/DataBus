package com.longyi.databus.daemon;

import java.util.List;

/**
 * Combiner convert  key:value-list into another key:value-list.
 * These processes can be used in Mapper to reduce intermediate data size. 
 * @author jiangbing
 *
 */
public abstract class Combiner<T> {
	public abstract List<T> combine(String key, List<T> values);
	
}
