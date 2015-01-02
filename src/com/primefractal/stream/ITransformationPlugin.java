/**
 * 
 */
package com.primefractal.stream;

import java.io.PipedReader;
import java.io.PipedWriter;

/**
 * @author GMan
 *
 */
public interface ITransformationPlugin {
	
	public abstract void wireUp(ITransformationPlugin plugin);

	public abstract void setPrimes(PipedReader primes);

	public abstract void setPrimesOut(PipedWriter primesOut);

	public abstract void setLowerOrderSet(PipedReader lowerOrderSet);

	public abstract void setHighOrderSet(PipedWriter highOrderSet);

	public abstract void setSetK(int setK);
	public int getSetK();

	public abstract void setReqSetSize(long reqSetSize);
	
	public abstract void setThisIsLastPluginInChain(boolean flag);
	
	public abstract boolean isThisIsLastPluginInChain();
	
	public final static int		PRIMES_PIPE_BUF_SIZE_		=1000000;	// 10^6
	public final static int		HIGH_ORDER_PIPE_BUF_SIZE_	=1000000;	// 10^6

}