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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import com.primefractal.main.PropertiesHelper;
import com.primefractal.utils.QueueUtils;

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
		
		// If nextPluginInChain is null, it means this instance plugin is last, and we don't need to use any outward facing queues at all
		if( nextPluginInChain == null ) {
			// These are unused
			setPrimesOutQ(null);
			setOutboundProcessedSetQ(null);
			return; 
		}
		
//		try {
			// Make the Primes Output Q
			primesOutQ=new ArrayBlockingQueue<Long>(PropertiesHelper.PRIMES_Q_BUF_SIZE_);
			// share it with the next plugin
			nextPluginInChain.setPrimesInQ(primesOutQ);
			
			// Make the Higher Order Set Queue
			outboundProcessedSetQ=new ArrayBlockingQueue<Long>(PropertiesHelper.HIGH_Q_BUF_SIZE_);
			// Share it with next plugin
			nextPluginInChain.setInboundSetToProcessQ(outboundProcessedSetQ);
			
//			
//			
//			// Make the PrimesOut Writer for us and the associated Reader for Delegate
//			PipedReader primesOutPipeReader=new PipedReader(PRIMES_PIPE_BUF_SIZE_);
//			nextPluginInChain.setPrimes(primesOutPipeReader);
//			// Now, wrap the Reader with the Writer, which is what this instance needs
//			PipedWriter primesOutPipeWriter = new PipedWriter(primesOutPipeReader);
//			this.setPrimesInQ(primesOutPipeWriter);
//			
//			
//			// Make the PrimesOut Writer for us and the associated Reader for Delegate
//			PipedReader highOrderPipeReader=new PipedReader(HIGH_ORDER_PIPE_BUF_SIZE_);
//			nextPluginInChain.setLowerOrderSet(highOrderPipeReader);
//			// Now, wrap the Reader with the Writer, which is what this instance needs
//			PipedWriter highOrderPipeWriter = new PipedWriter(highOrderPipeReader);
//			this.setHighOrderSet(highOrderPipeWriter);			
//			
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	}
	

	/* 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		boolean processComplete=false;
		Long lowOrderSetElemLong=null;
		//long nextPrimeIndex=0L;
		Long nextPrimeIndexLong=null;
		

		// Read first Prime from Prime Reader
		nextPrimeIndexLong=getFromPrimesInQ();
		if( nextPrimeIndexLong == PropertiesHelper.EOF_FOR_QUEUE_ ) {
			// This should never occur.  End of file encountered on first element of the Ordered Set of Primes.
			if( thisIsLastPluginInChain == false ) {
				putToPrimesOutQ(PropertiesHelper.EOF_FOR_QUEUE_);
				putToOutboundProcessedSetQ(PropertiesHelper.EOF_FOR_QUEUE_);
			}
			closeResultsFile();
			return;
		}
		//nextPrimeIndex=new Long(nextPrimeIndexLong).longValue();	
		// Write the read element to Reader of Plugin, unless we are the last Plugin.
		if( thisIsLastPluginInChain == false )
			putToPrimesOutQ(nextPrimeIndexLong);
			//primesOut.write(nextPrimeIndexLong+NEWLINE_);

		
		long maxElemInResSetRequested=PropertiesHelper.getInstance().getFixedElementCountInResultSet();
		long highOrderSetCounter=0;  // The number of elems added to the next set
		long processCounter=0;
		while( processComplete == false ) {
			//System.out.println("DEBUG:{"+nextPrimeIndex+", "+nextProcessValueStr+", "+processCounter+"}\n");

			// Read next value from low order set (the set to be transformed)
			lowOrderSetElemLong=getFromInboundSetToProcessQ();
			if( lowOrderSetElemLong == PropertiesHelper.EOF_FOR_QUEUE_ ) {
				// Let the consumer of the stream know the end of file has been reached
				if( thisIsLastPluginInChain == false ) {
					putToOutboundProcessedSetQ(PropertiesHelper.EOF_FOR_QUEUE_);
				}
				closeResultsFile();
				// Don't flush the primesOut Writer because he still has plenty of elements to transfer !
				processComplete=true;
				continue;
			}
			processCounter++;
			
			// Ensure the nextPrimeIndex to check, is >= processCounter
			// Ensure the next Prime Index is > current counter.  Must use < and not <= otherwise search for index beyond stream
			while( nextPrimeIndexLong.longValue() < processCounter) {
				nextPrimeIndexLong=getFromPrimesInQ();
				//nextPrimeIndex=new Long(nextPrimeIndexLong).longValue();
				// Write the read element to Reader of Plugin, unless we are the last Plugin.
				if( thisIsLastPluginInChain == false )
					putToPrimesOutQ(nextPrimeIndexLong);
			}
			
			// When the index (e.g. counter) of the Low Order Set equals a Prime number, we have a Prime Index.
			//   and therefore, the element is a member of the next (higher order) set.
			if( nextPrimeIndexLong.longValue() == processCounter) {
				//System.out.println("Writing:{"+nextPrimeIndex+", "+nextProcessValueStr+", "+processCounter+"}\n");
				try {
					String strToWrite=new String(lowOrderSetElemLong.toString() + NEWLINE_);
					resultsFile.write(strToWrite);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				highOrderSetCounter++;

				// Write the read element to Reader of next Thread (e.g. he is a member of the higherOrderSet)
				if( thisIsLastPluginInChain == false ) {
					putToOutboundProcessedSetQ(lowOrderSetElemLong);
				}
				
				// if maxElemInResSetRequested == 0, we don't cap the output and therefore don't need to check
				if( (maxElemInResSetRequested != 0L ) && (highOrderSetCounter >= maxElemInResSetRequested) ) {
					// There may be more, but the requester has asked that we only capture the first maxElemInResSetRequested in our result files
					closeResultsFile();
					// Don't flush the primesOut Writer because he still has plenty of elements to transfer !
					processComplete=true;
				}
			}

		} // while


		if( isThisIsLastPluginInChain() == true ) {
			// Nothing to do - we're done
		} else {
			// We need to read the rest of the Primes Q and push entries downstream, including EOF
			nextPrimeIndexLong=getFromPrimesInQ();
			while( nextPrimeIndexLong != PropertiesHelper.EOF_FOR_QUEUE_ ) {
				putToPrimesOutQ(nextPrimeIndexLong);
				nextPrimeIndexLong=getFromPrimesInQ();
			}
			putToPrimesOutQ(PropertiesHelper.EOF_FOR_QUEUE_);
		}
//		nextPrimeIndexLong=getFromPrimesInQ();
//		while( nextPrimeIndexLong != PropertiesHelper.EOF_FOR_QUEUE_ ) {
//			// Write the read element to Reader of Plugin
//			if( thisIsLastPluginInChain == false ) {
//				putToPrimesOutQ(nextPrimeIndexLong);
//			}
//			nextPrimeIndexLong=getFromPrimesInQ();
//		}
//
//		if( isThisIsLastPluginInChain() == false )
//			putToPrimesOutQ(PropertiesHelper.EOF_FOR_QUEUE_);
		
	}
	
//	protected void workerClose() {
//		// Close our end of the Pipes
//		try {
//			primes.close();
//			primesOut.close();
//			lowerOrderSet.close();
//			highOrderSet.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		// Close our output file
//	}
	
	@Override
	public void setPrimesInQ(BlockingQueue<Long> primesQ) {
		// TODO Auto-generated method stub
		primesInQ=primesQ;
	}

	@Override
	public void setPrimesOutQ(BlockingQueue<Long> primesOutQ) {
		// TODO Auto-generated method stub
		this.primesOutQ=primesOutQ;
	}

	@Override
	public void setInboundSetToProcessQ(BlockingQueue<Long> lowerOrderQ) {
		inboundSetToProcessQ=lowerOrderQ;
	}

	@Override
	public void setOutboundProcessedSetQ(BlockingQueue<Long> higherOrderQ) {
		// TODO Auto-generated method stub
		outboundProcessedSetQ=higherOrderQ;
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
	
	// READING FROM QUEUES
	protected Long getFromPrimesInQ() {
		return( QueueUtils.getLongFromQueue(primesInQ) );
	}
	protected Long getFromInboundSetToProcessQ() {
		return( QueueUtils.getLongFromQueue(inboundSetToProcessQ) );
	}
//	private Long getLongFromQueue(BlockingQueue<Long> queue) {
//		Long nextValue=null;
//		boolean done=false;
//		
//		while( done == false ) {
//			boolean pollResult=false;
//			try {
//				while(pollResult == false) {
//					nextValue=queue.poll(PropertiesHelper.POLL_DURATION_, TimeUnit.MILLISECONDS);
//					Thread.sleep(PropertiesHelper.Q_POLL_SLEEP_DURATION_);
//				}
//				done=true;
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
//		return(nextValue);
//	}
	
	// WRITING TO QUEUES
	protected void putToPrimesOutQ(Long valueToPut) {
		QueueUtils.putLongToQueue(primesOutQ, valueToPut);
	}
	protected void putToOutboundProcessedSetQ(Long valueToPut) {
		QueueUtils.putLongToQueue(outboundProcessedSetQ, valueToPut);
	}
//	private void putLongToQueue(BlockingQueue<Long> queue, Long valueToPut) {
//		boolean done=false;
//		
//		while( done == false ) {
//			boolean offerResult=false;
//			try {
//				offerResult=queue.offer(valueToPut);
//				while(offerResult == false) {
//					Thread.sleep(PropertiesHelper.Q_OFFER_SLEEP_DURATION_);
//					offerResult=queue.offer(valueToPut);
//				}
//				done=true;
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		}
			
//	}
		
		
////		StringBuffer sb=new StringBuffer();
////		int	castedNewlineChar=(int)NEWLINE_CHAR_;
////		int value=0;		
////		try {
////
////			if(fromReader == PRIME_READER_) {
////				value=primes.read();
////				while ( ((char)value != castedNewlineChar) && ((char)value != (char)EOF_) ) {
////					sb.append((char)value);
////					value=primes.read();
////				}	
////			} else {
////				value=lowerOrderSet.read();
////				while ( ((char)value != castedNewlineChar) && ((char)value != (char)EOF_) ) {
////					sb.append((char)value);
////					value=lowerOrderSet.read();
////				}
////			}
//			
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			//This is actually ok and handled gracefully with the return of an empty string.
//			System.err.println("Note: this may not affect results");
//			System.err.println("Error: Current thread was "+Thread.currentThread().getName()+" offending char was "+value);
//			e.printStackTrace();
//			
//		}
//		return sb.toString();
//	}


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
	public void setThisIsLastPluginInChain(boolean flag) {
		// TODO Auto-generated method stub
		thisIsLastPluginInChain=flag;
	}
	@Override
	public boolean isThisIsLastPluginInChain() {
		// TODO Auto-generated method stub
		return thisIsLastPluginInChain;
	}




//	protected 	PipedReader 			primes;
//	protected 	PipedWriter				primesOut;
//	protected 	PipedReader 			lowerOrderSet;
//	protected	PipedWriter				highOrderSet;
	
	protected	BlockingQueue<Long>		primesInQ;
	protected	BlockingQueue<Long>		primesOutQ;
	protected	BlockingQueue<Long>		inboundSetToProcessQ;
	protected	BlockingQueue<Long>		outboundProcessedSetQ;

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


//	/* 
//	 * @see com.primefractal.stream.ITransformationPlugin#setLowerOrderQueue(java.util.concurrent.BlockingQueue)
//	 */
//	@Override
//	public void setLowerOrderQueue(BlockingQueue<Long> lowerOrderQ) {
//		// TODO Auto-generated method stub
//		
//	}

//	/* 
//	 * @see com.primefractal.stream.ITransformationPlugin#setHighOrderSet(java.io.PipedWriter)
//	 */
//	@Override
//	public void setHighOrderSet(PipedWriter highOrderSet) {
//		// TODO Auto-generated method stub
//		
//	}
}
