/**
 * 
 */
package com.primefractal.utils;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.primefractal.main.PropertiesHelper;

/**
 * @author GMan
 *
 */
public class QueueUtils {
	
	// READING FROM QUEUES
	public static Long getLongFromQueue(BlockingQueue<Long> queue) {
		Long nextValue=null;
		boolean done=false;
		
		while( done == false ) {
			try {
				nextValue=queue.poll();
				while(nextValue == null) {
					//nextValue=queue.poll(PropertiesHelper.POLL_DURATION_, TimeUnit.MILLISECONDS);
					Thread.sleep(PropertiesHelper.Q_POLL_SLEEP_DURATION_);
					nextValue=queue.poll();
					
				}
				done=true;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return(nextValue);
	}
	
	// WRITING TO QUEUES
	public static void putLongToQueue(BlockingQueue<Long> queue, Long valueToPut) {
		boolean done=false;
		
		while( done == false ) {
			boolean offerResult=false;
			try {
				offerResult=queue.offer(valueToPut);
				while(offerResult == false) {
					Thread.sleep(PropertiesHelper.Q_OFFER_SLEEP_DURATION_);
					offerResult=queue.offer(valueToPut);
				}
				done=true;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
			
	}

}
