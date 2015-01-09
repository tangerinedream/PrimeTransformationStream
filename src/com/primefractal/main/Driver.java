/**
 * 
 */
package com.primefractal.main;

import java.io.Reader;
import java.util.ArrayList;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.primefractal.stream.ITransformationPlugin;
import com.primefractal.stream.TransformationWorker;
import com.primefractal.utils.IOUtils;
import com.primefractal.utils.QueueUtils;

/**
 * @author GMan
 *
 */
public class Driver {

	protected ArrayList<ITransformationPlugin> makeTransformationWorkers(PropertiesHelper props, BlockingQueue<Long> primesQ, BlockingQueue<Long> lowerOrderQ) {
		// Get the number of transformations or K  
		
		
		// The number of plugins to install = "K-1".  K==1 is already generated by the input stream (e.g. PrimeSeive)
		int nbrPluginsRequired=props.getMaxK() - 1;
		long sizeOfIntegerSet=props.getSizeOfIntegerSet();
		
		ArrayList<ITransformationPlugin>  plugins=new ArrayList<ITransformationPlugin>();
		for(int i=0; i < nbrPluginsRequired; i++ ) {
			// we add 2 to i because that is the K value for this worker.  Recall, Set 0 doesn't exist and Set 1 is the input stream to the java program
			ITransformationPlugin currPlugin=new TransformationWorker(i+2, sizeOfIntegerSet);
			plugins.add(currPlugin);
		}
		
		// Flag the last plugin as being the last in the chain.  Java counts starting at zero, so subtract one
		ITransformationPlugin currPlugin=plugins.get(nbrPluginsRequired-1);
		currPlugin.setThisIsLastPluginInChain(true);
		
		// Now wire them up
		for(int i=0; i < nbrPluginsRequired; i++ ) {
			
			currPlugin=plugins.get(i);
			
			if( i == 0 ) {
				// Special case - no plugin ahead of him to wire him up.  We will do it manually.
				currPlugin.setPrimesInQ(primesQ);
				currPlugin.setInboundSetToProcessQ(lowerOrderQ);
			}

			if( currPlugin.isThisIsLastPluginInChain() == true ) {
				// Last element in the list - special case
				currPlugin.wireUp(null);
			} else {
				currPlugin.wireUp(plugins.get(i+1));
			}
		}
		
		
		return(plugins);
	}
	
	
	// Just for tidy purposes....
	protected PropertiesHelper setup(String args[]) {
		PropertiesHelper props=PropertiesHelper.getInstance();

		
		if( args.length != 1 ) {
			System.err.println("Please add a command line parameter signifying the Number_of_Integers_in_Set_0.  Exiting. ");
			System.exit(-1);
		} else {
			PropertiesHelper.getInstance().setSizeOfIntegerSet(new Long( args[SIZE_OF_INT_SET_ARGS_IDX_] ).longValue());
		}
		
		LOGGER_.info("K=0 stream ["+PropertiesHelper.getInstance().getSizeOfIntegerSet()+"] elements. " +
				"Transforms to K=["+PropertiesHelper.getInstance().getMaxK() + "] " +
				"Capping at ["+PropertiesHelper.getInstance().getFixedElementCountInResultSet()+"] (note: 0=no cap)" );

		
		// Are we using stdin for K=1 (Set.1.xxx) or a File Override?
		if(props.isUseFileInputStream() == true )
			setK1Reader=IOUtils.makeSetOneReaderWithDefaultFilename();
		else
			setK1Reader=IOUtils.makeSetOneReaderFromStdin();
		
		return(props);
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		LOGGER_.setLevel(Level.INFO);
		long sTime=System.currentTimeMillis();
		
		Driver driver=new Driver();
		PropertiesHelper props=driver.setup(args);
		
		// Make Queues Driver to share with the first plugin (a special case)
		primesQueue=new ArrayBlockingQueue<Long>(QueueUtils.PRIMES_Q_BUF_SIZE_);
		lowerOrderQueue=new ArrayBlockingQueue<Long>(QueueUtils.HIGH_Q_BUF_SIZE_);


		// Make and wire up the Workers
		ArrayList<ITransformationPlugin> plugins=driver.makeTransformationWorkers(props, primesQueue, lowerOrderQueue);
		
		//Launch Threads...
		for( int i=0; i < plugins.size(); i++ ) {
			ITransformationPlugin currPlugin=plugins.get(i);
			Thread t=new Thread((Runnable) currPlugin);
			t.setName("K="+new Integer(currPlugin.getSetK()).toString());
			t.start();
		}
		
		// Feed Plugin with Set K=1 from stream
		driver.processPrimesStream();

		long eTime=System.currentTimeMillis();
		LOGGER_.info("(Main Thread Time only) Total time taken was "+ (eTime-sTime) +" ms");
	}
	
	// WRITING TO QUEUES
	protected void putToPrimesOutQ(Long valueToPut) {
		QueueUtils.putLongToQueue(primesQueue, valueToPut);
	}
	protected void putToOutboundProcessedSetQ(Long valueToPut) {
		QueueUtils.putLongToQueue(lowerOrderQueue, valueToPut);
	}
	
	protected void processPrimesStream() {
		Long nextPrimeRead=IOUtils.getLongFromFile(setK1Reader);
		
		// When EOF is encountered on the input stream, getNextPrime() will catch that and return EOF_FOR_QUEUE_ instead
		while(nextPrimeRead != QueueUtils.EOF_FOR_QUEUE_) {
			//BlockingQueue<Long> copyPrimesQ=primesQueue;        // Used for eclipse debugging purposes only
			//BlockingQueue<Long> copyOutboundQ=lowerOrderQueue;  // Used for eclipse debugging purposes only

			putToOutboundProcessedSetQ(nextPrimeRead);
			putToPrimesOutQ(nextPrimeRead);
			Thread.yield();
			nextPrimeRead=IOUtils.getLongFromFile(setK1Reader);
		}
		
		// Let 'em know we're done here
		putToPrimesOutQ(QueueUtils.EOF_FOR_QUEUE_);
		putToOutboundProcessedSetQ(QueueUtils.EOF_FOR_QUEUE_);
	}
		
	
	protected static BlockingQueue<Long>	primesQueue=null;
	protected static BlockingQueue<Long>	lowerOrderQueue=null;

	
	// The stream of primes (e.g. K=1)
	protected static	Reader setK1Reader=null; 
	
	final private static int 	SIZE_OF_INT_SET_ARGS_IDX_=0;
	final private static Logger LOGGER_=Logger.getLogger("PrimeTransformationStream");  // project wide logger
}
