/**
 * 
 */
package com.primefractal.stream;

import java.io.PipedReader;
import java.io.PipedWriter;
import java.util.concurrent.BlockingQueue;

/**
 * @author GMan
 *
 */
public interface ITransformationPlugin {
	
	public abstract void wireUp(ITransformationPlugin plugin);

	public abstract void setPrimesInQ(BlockingQueue<Long> primesQ);
	public abstract void setPrimesOutQ(BlockingQueue<Long> primesOutQ);
	public abstract void setInboundSetToProcessQ(BlockingQueue<Long> lowerOrderQ);
	public abstract void setOutboundProcessedSetQ(BlockingQueue<Long> higherOrderQ);


	public abstract void setSetK(int setK);
	public int getSetK();

	public abstract void setReqSetSize(long reqSetSize);	
	public abstract void setThisIsLastPluginInChain(boolean flag);	
	public abstract boolean isThisIsLastPluginInChain();
}