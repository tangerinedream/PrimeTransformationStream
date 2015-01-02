/**
 * 
 */
package com.primefractal.stream;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.io.Writer;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import com.primefractal.main.PropertiesHelper;

/**
 * @author GMan
 *
 */
public class TransformationWorker implements Runnable, ITransformationPlugin {
	
	public TransformationWorker(int K, long reqSetSize2) {
		super();
		setSetK(K);
		setReqSetSize(reqSetSize2);
		makeOutFile();
	}
	
	public void wireUp( /* PipedReader primes, PipedReader setToTransform, */ ITransformationPlugin nextPluginInChain /* PipedWriter primesOut, PipedWriter higherOrderSet */) {
		// This instances Readers were set by its Predecessor in the chain, so nothing to do here.
		
		// For output pipes, this class creates both the buffer and the Pipes (both sides) and hands to the next plugin in the chain.
		// So, we need to do some work here
		
		// If nextPluginInChain is null, it means this instance plugin is last, and we don't need to use any PipedWriters at all
		if( nextPluginInChain == null ) {
			// These are unused
			setPrimesOut(null);
			setHighOrderSet(null);
			return; 
		}
		
		try {
			
			// Make the PrimesOut Writer for us and the associated Reader for Delegate
			PipedReader primesOutPipeReader=new PipedReader(PRIMES_PIPE_BUF_SIZE_);
			nextPluginInChain.setPrimes(primesOutPipeReader);
			// Now, wrap the Reader with the Writer, which is what this instance needs
			PipedWriter primesOutPipeWriter = new PipedWriter(primesOutPipeReader);
			this.setPrimesOut(primesOutPipeWriter);
			
			
			// Make the PrimesOut Writer for us and the associated Reader for Delegate
			PipedReader highOrderPipeReader=new PipedReader(HIGH_ORDER_PIPE_BUF_SIZE_);
			nextPluginInChain.setLowerOrderSet(highOrderPipeReader);
			// Now, wrap the Reader with the Writer, which is what this instance needs
			PipedWriter highOrderPipeWriter = new PipedWriter(highOrderPipeReader);
			this.setHighOrderSet(highOrderPipeWriter);			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

	/* 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		boolean processComplete=false;
		String lowOrderSetElemStr=null;
		long nextPrimeIndex=0L;
		String nextPrimeIndexStr=null;
		
		try {
			// Read first Prime from Prime Reader
			nextPrimeIndexStr=getStringFromReader(PRIME_READER_);
			if( nextPrimeIndexStr.compareTo("") == 0 ) {
				// This should never occur.  End of file encountered on first element of the Ordered Set of Primes.
				if( thisIsLastPluginInChain == false ) {
					eofWriter(primesOut);
					eofWriter(highOrderSet);
				}
				closeResultsFile();
				return;
			}
			nextPrimeIndex=new Long(nextPrimeIndexStr).longValue();	
			// Write the read element to Reader of Plugin, unless we are the last Plugin.
			if( thisIsLastPluginInChain == false )
				primesOut.write(nextPrimeIndexStr+NEWLINE_);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		long maxElemInResSetRequested=PropertiesHelper.getInstance().getFixedElementCountInResultSet();
		long highOrderSetCounter=0;  // The number of elems added to the next set
		long processCounter=0;
		while( processComplete == false ) {
			//System.out.println("DEBUG:{"+nextPrimeIndex+", "+nextProcessValueStr+", "+processCounter+"}\n");
			
			try {
				// Read next value from low order set (the set to be transformed)
				lowOrderSetElemStr=getStringFromReader(LOW_ORDER_SET_READER_);
				if( lowOrderSetElemStr.compareTo("") == 0 ) {
					// Let the consumer of the stream know the end of file has been reached
					if( thisIsLastPluginInChain == false ) {
						eofWriter(highOrderSet);
					}
					closeResultsFile();
					// Don't flush the primesOut Writer because he still has plenty of elements to transfer !
					processComplete=true;
					continue;
				}
				processCounter++;
				
				// Ensure the nextPrimeIndex to check, is >= processCounter
				// Ensure the next Prime Index is > current counter.  Must use < and not <= otherwise search for index beyond stream
				while( nextPrimeIndex < processCounter) {
					nextPrimeIndexStr=getStringFromReader(PRIME_READER_);
					nextPrimeIndex=new Long(nextPrimeIndexStr).longValue();
					// Write the read element to Reader of Plugin, unless we are the last Plugin.
					if( thisIsLastPluginInChain == false )
						primesOut.write(nextPrimeIndexStr+NEWLINE_);
				}
				
				// When the index (e.g. counter) of the Low Order Set equals a Prime number, we have a Prime Index.
				//   and therefore, the element is a member of the next (higher order) set.
				if( nextPrimeIndex == processCounter) {
					//System.out.println("Writing:{"+nextPrimeIndex+", "+nextProcessValueStr+", "+processCounter+"}\n");
					resultsFile.write(lowOrderSetElemStr+NEWLINE_);
					highOrderSetCounter++;
					// Write the read element to Reader of next Thread (e.g. he is a member of the higherOrderSet)
					if( thisIsLastPluginInChain == false ) {
						highOrderSet.write(lowOrderSetElemStr+NEWLINE_);
					}
					
					// if maxElemInResSetRequested == 0, we don't cap the output and therefore don't need to check
					if( (maxElemInResSetRequested != 0L ) && highOrderSetCounter >= maxElemInResSetRequested ) {
						// There may be more, but the requester has asked that we only capture the first maxElemInResSetRequested in our result files
						closeResultsFile();
						// Don't flush the primesOut Writer because he still has plenty of elements to transfer !
						processComplete=true;
					}
				}

				
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} // while

		try {
			// Write the rest of the primesIn stream to primesOut so next thread can continue.
			// There is a case where we may enter the condition of no primes left to send, when we've exhausted the inbound stream
			if( thisIsLastPluginInChain == false ) {
				nextPrimeIndexStr=getStringFromReader(PRIME_READER_);
				while( nextPrimeIndexStr.compareTo("") != 0 ) {
					// Write the read element to Reader of Plugin
					primesOut.write(nextPrimeIndexStr+NEWLINE_);
					nextPrimeIndexStr=getStringFromReader(PRIME_READER_);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			LOGGER_.severe("Failed to push remaining Ordered Set of Primes (k=1) through pipe.  Fatal error - exiting");
			System.exit(-1);
		}
		if( isThisIsLastPluginInChain() == false )
			eofWriter(primesOut);
	}
	
	protected void workerClose() {
		// Close our end of the Pipes
		try {
			primes.close();
			primesOut.close();
			lowerOrderSet.close();
			highOrderSet.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Close our output file
	}
	
	/**
	 * 
	 */
	private void makeOutFile() {
		String filename=SET_PREFIX_+DELIM_+getSetK()+DELIM_+getReqSetSize()+".gz";
				
		try {
			// 2. Open the FileOutputStream
			fos=new FileOutputStream(filename);
			bos=new BufferedOutputStream(fos);
			
			// 3. Wrap fos into GZIP Filter Output Stream
			gOut=new GZIPOutputStream(bos);
			
			// 4. Wrap gOut stream with BitOutputStream so we can stream booleans
			resultsFile=new OutputStreamWriter(gOut);
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	private void closeResultsFile() {
		try {
			resultsFile.close();
			gOut.close();
			bos.close();
			fos.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void eofWriter(Writer writer) {
		try {
			writer.write(EOF_);
			writer.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	protected String getStringFromReader(boolean fromReader) {
		StringBuffer sb=new StringBuffer();
		int	castedNewlineChar=(int)NEWLINE_CHAR_;
		
		try {
			int value;
			if(fromReader == PRIME_READER_) {
				value=primes.read();
				while ( ((char)value != castedNewlineChar) && ((char)value != (char)EOF_) ) {
					sb.append((char)value);
					value=primes.read();
				}	
			} else {
				value=lowerOrderSet.read();
				while ( ((char)value != castedNewlineChar) && ((char)value != (char)EOF_) ) {
					sb.append((char)value);
					value=lowerOrderSet.read();
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			//This is actually ok and handled gracefully with the return of an empty string.
			//e.printStackTrace();
		}
		return sb.toString();
	}

	@Override
	public void setPrimes(PipedReader primes) {
		this.primes = primes;
	}

	@Override
	public void setPrimesOut(PipedWriter primesOut) {
		this.primesOut = primesOut;
	}

	@Override
	public void setLowerOrderSet(PipedReader lowerOrderSet) {
		this.lowerOrderSet = lowerOrderSet;
	}

	@Override
	public void setSetK(int setK) {
		this.setK = setK;
	}

	@Override
	public void setReqSetSize(long reqSetSize) {
		this.reqSetSize = reqSetSize;
	}
	
	public int getSetK() {
		return (setK);
	}

	public long getReqSetSize() {
		return (reqSetSize);
	}
	

	@Override
	public void setHighOrderSet(PipedWriter highOrderSet) {
		this.highOrderSet = highOrderSet;
	}


	@Override
	public void setThisIsLastPluginInChain(boolean flag) {
		// TODO Auto-generated method stub
		thisIsLastPluginInChain=flag;
	}
	@Override
	public boolean isThisIsLastPluginInChain() {
		// TODO Auto-generated method stub
		return thisIsLastPluginInChain;
	}




	protected 	PipedReader 			primes;
	protected 	PipedWriter				primesOut;
	protected 	PipedReader 			lowerOrderSet;
	protected	PipedWriter				highOrderSet;

	protected 	int						setK;
	protected 	long					reqSetSize;
	protected	boolean					thisIsLastPluginInChain=false;
	
	private 	FileOutputStream		fos;
	private 	BufferedOutputStream  	bos;
	private 	GZIPOutputStream		gOut;
	private 	OutputStreamWriter		resultsFile;
	
	private final static String 	SET_PREFIX_="Set";
	private final static String 	NEWLINE_="\n";
	private final static char   	NEWLINE_CHAR_='\n';
	private final static String 	DELIM_=".";
	private final static int		EOF_=-1;
	private final static boolean 	PRIME_READER_=true;
	private final static boolean 	LOW_ORDER_SET_READER_=false;
	
	
	final private static Logger LOGGER_=Logger.getLogger("PrimeTransformationStream");  // project wide logger
}
