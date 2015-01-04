/**
 * 
 */
package com.primefractal.stream;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;
import com.primefractal.main.PropertiesHelper;
import com.primefractal.utils.IOUtils;
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
		IOUtils.makeProcessedFileWriterWithDefaultFilename(getSetK());
	}
	
	public void wireUp(ITransformationPlugin nextPluginInChain) {
		
		// If nextPluginInChain is null, it means this instance plugin is last, and we don't need to use any outward facing queues at all
		if( nextPluginInChain == null ) {
			// These are unused
			setPrimesOutQ(null);
			setOutboundProcessedSetQ(null);
			return; 
		}
		

		// Make the Primes Output Q
		primesOutQ=new ArrayBlockingQueue<Long>(QueueUtils.PRIMES_Q_BUF_SIZE_);
		// share it with the next plugin
		nextPluginInChain.setPrimesInQ(primesOutQ);
		
		// Make the Higher Order Set Queue
		outboundProcessedSetQ=new ArrayBlockingQueue<Long>(QueueUtils.HIGH_Q_BUF_SIZE_);
		// Share it with next plugin
		nextPluginInChain.setInboundSetToProcessQ(outboundProcessedSetQ);
	}
	

	/* 
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		boolean processComplete=false;
		Long lowOrderSetElemLong=null;
		Long nextPrimeIndexLong=null;
		//long debugCntPrimesRead=0;
		//long debugCntInSetElemRead=0;
		

		// Read first Prime from Prime Reader
		nextPrimeIndexLong=getFromPrimesInQ();
		//debugCntPrimesRead++;
		if( nextPrimeIndexLong == QueueUtils.EOF_FOR_QUEUE_ ) {
			// This should never occur.  End of file encountered on first element of the Ordered Set of Primes.
			if( thisIsLastPluginInChain == false ) {
				putToPrimesOutQ(QueueUtils.EOF_FOR_QUEUE_);
				putToOutboundProcessedSetQ(QueueUtils.EOF_FOR_QUEUE_);
			}
			//closeResultsFile();
			try {
				resultsFile.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return;
		}

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
//			debugCntInSetElemRead++;
//			System.out.println("DEBUG: NbrPrimesRead="+debugCntPrimesRead+" Nbr InSetElemsRead="+debugCntInSetElemRead);
//			System.out.println("DEBUG: PrimesQ Capacity="+primesInQ.remainingCapacity()+" SetInQ capacity="+inboundSetToProcessQ.remainingCapacity());
			if( lowOrderSetElemLong == QueueUtils.EOF_FOR_QUEUE_ ) {
				// Let the consumer of the stream know the end of file has been reached
				if( thisIsLastPluginInChain == false ) {
					putToOutboundProcessedSetQ(QueueUtils.EOF_FOR_QUEUE_);
				}
				//closeResultsFile();
				try {
					resultsFile.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				// Don't flush the primesOut Writer because he still has plenty of elements to transfer !
				processComplete=true;
				continue;
			}
			processCounter++;
			

			// Ensure the next Prime Index is > processCounter.  Must use < and not <= otherwise search for index beyond stream
			while( nextPrimeIndexLong.longValue() < processCounter) {
				nextPrimeIndexLong=getFromPrimesInQ();
				//debugCntPrimesRead++;
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
					// Note: Don't flush the primesOut Writer because he still has plenty of elements to transfer !
					processComplete=true;
					try {
						resultsFile.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}

		} // while
		//LOGGER_.info("Worker K=["+getSetK()+"] completed, processing ["+processCounter+"] elements in inboundSetToProcessQ");

		if( isThisIsLastPluginInChain() == true ) {
			// Nothing to do - we're done
		} else {
			// We need to read the rest of the Primes Q and push entries downstream, including EOF
			nextPrimeIndexLong=getFromPrimesInQ();
			while( nextPrimeIndexLong != QueueUtils.EOF_FOR_QUEUE_ ) {
				putToPrimesOutQ(nextPrimeIndexLong);
				nextPrimeIndexLong=getFromPrimesInQ();
			}
			putToPrimesOutQ(QueueUtils.EOF_FOR_QUEUE_);
		}
	}
		
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
//	private void makeOutFile() {
//		String filename=SET_PREFIX_+DELIM_+getSetK()+DELIM_+getReqSetSize()+".gz";
//				
//		try {
//			// 2. Open the FileOutputStream
//			fos=new FileOutputStream(filename);
//			bos=new BufferedOutputStream(fos);
//			
//			// 3. Wrap fos into GZIP Filter Output Stream
//			gOut=new GZIPOutputStream(bos);
//			
//			// 4. Wrap gOut stream with BitOutputStream so we can stream booleans
//			resultsFile=new OutputStreamWriter(gOut);
//			
//		} catch (FileNotFoundException e) {
//			e.printStackTrace();
//		} catch (IOException e) {
//			e.printStackTrace();
//		}
//		
//	}
//	private void closeResultsFile() {
//		try {
//			resultsFile.close();
//			gOut.close();
//			bos.close();
//			fos.close();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}
	
	// READING FROM QUEUES
	protected Long getFromPrimesInQ() {
		return( QueueUtils.getLongFromQueue(primesInQ) );
	}
	protected Long getFromInboundSetToProcessQ() {
		return( QueueUtils.getLongFromQueue(inboundSetToProcessQ) );
	}
	
	// WRITING TO QUEUES
	protected void putToPrimesOutQ(Long valueToPut) {
		QueueUtils.putLongToQueue(primesOutQ, valueToPut);
	}
	protected void putToOutboundProcessedSetQ(Long valueToPut) {
		QueueUtils.putLongToQueue(outboundProcessedSetQ, valueToPut);
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
	public void setThisIsLastPluginInChain(boolean flag) {
		// TODO Auto-generated method stub
		thisIsLastPluginInChain=flag;
	}
	@Override
	public boolean isThisIsLastPluginInChain() {
		// TODO Auto-generated method stub
		return thisIsLastPluginInChain;
	}


	
	protected	BlockingQueue<Long>		primesInQ;
	protected	BlockingQueue<Long>		primesOutQ;
	protected	BlockingQueue<Long>		inboundSetToProcessQ;
	protected	BlockingQueue<Long>		outboundProcessedSetQ;

	protected 	int						setK;
	protected 	long					reqSetSize;
	protected	boolean					thisIsLastPluginInChain=false;
	
//	private 	FileOutputStream		fos;
//	private 	BufferedOutputStream  	bos;
//	private 	GZIPOutputStream		gOut;
	private 	OutputStreamWriter		resultsFile;
	
	private final static String 	SET_PREFIX_="Set";
	private final static String 	NEWLINE_="\n";
	private final static String 	DELIM_=".";
	
	final private static Logger LOGGER_=Logger.getLogger("PrimeTransformationStream");  // project wide logger
}
