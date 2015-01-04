/**
 * 
 */
package com.primefractal.stream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;

import com.primefractal.main.PropertiesHelper;
import com.primefractal.utils.IOUtils;

/**
 * @author GMan
 *
 */
public class SetTransformation {
	
	protected void crunch(Reader primes, Reader lowerOrderSet, Writer higherOrderSet) {	
		
		// Get First Prime from Prime Reader
		Long nextPrimeIndex=0L;
		nextPrimeIndex=IOUtils.getLongFromFile(primes);
		
		// Do the transformation, potentially limiting the results by elemCountLimitForSet
		long elemCountLimitForSet=PropertiesHelper.getInstance().getFixedElementCountInResultSet();
		long highOrderSetCounter=0L; // The number of elems promoted to the higher order set
		Long currElemToProcess=0L;
		long processCounter=1L;
		boolean processComplete=false;
		while( processComplete == false ) {
			//System.out.println("DEBUG:{"+nextPrimeIndex+", "+nextProcessValueStr+", "+processCounter+"}\n");
			
			// Get next element to process from Lower Order Set
			currElemToProcess=IOUtils.getLongFromFile(lowerOrderSet);
			if( currElemToProcess == IOUtils.EOF_LONG_ ) {
				processComplete=true;
				continue;
			}
			// Ensure the nextPrimeIndex to check, is >= processCounter
			// Ensure the next Prime Index is > current counter.  Must use < and not <= otherwise search for index beyond stream
			while( nextPrimeIndex < processCounter) {
				nextPrimeIndex=IOUtils.getLongFromFile(primes);
			}
		
			// if the same, this value is a member of the next set
			if( nextPrimeIndex == processCounter) {
				//System.out.println("Writing:{"+nextPrimeIndex+", "+nextProcessValueStr+", "+processCounter+"}\n");
				String elemToPromoteToHigherOrderSet=new String(currElemToProcess.toString() + IOUtils.NEWLINE_STR_);
				try {
					higherOrderSet.write(elemToPromoteToHigherOrderSet);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				highOrderSetCounter++;
			}
			
			// if maxElemInResSetRequested == 0, we don't cap the output and therefore don't need to check
			if( (elemCountLimitForSet != 0L ) && (highOrderSetCounter >= elemCountLimitForSet) ) {
				// There may be more, but the requester has asked that we only capture the first maxElemInResSetRequested in our result files
				// Note: Don't flush the primesOut Writer because he still has plenty of elements to transfer !
				processComplete=true;
				try {
					higherOrderSet.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}	
			}
			
			// Move the chains for the set we are processing
			processCounter++;
		}
	}	
	
	
	public static void main(String args[]) {
		
		// Process args: SetSize LowerK HigherK
		String reqSetSize=args[0];
		PropertiesHelper.getInstance().setSizeOfIntegerSet(new Long(reqSetSize).longValue());
		String kLowerOrderSetStr=args[1];
		long kLowerOrderSetNbr=new Long(kLowerOrderSetStr).longValue();
		String kHigherOrderSetStr=args[2];
		long kHigherOrderSetNbr=new Long(kHigherOrderSetStr).longValue();
		
		// Are we using stdin for K=1 (Set.1.xxx) or a File Override?
		Reader primesReader=null;
		if(PropertiesHelper.getInstance().isUseFileInputStream() == true )
			primesReader=IOUtils.makeSetOneReaderWithDefaultFilename();
		else
			primesReader=IOUtils.makeSetOneReaderFromStdin();

		// Make Reader for LowerOrderSet
		Reader lowerOrderSet=IOUtils.makeInputSetReaderWithDefaultFilename(kLowerOrderSetNbr);
		
		// Make Writer for HigherOrderSet
		Writer higherOrderSet=IOUtils.makeProcessedFileWriterWithDefaultFilename(kHigherOrderSetNbr);
		
		// Perform the Transformation
		SetTransformation st=new SetTransformation();
		st.crunch(primesReader, lowerOrderSet, higherOrderSet);
		
		try {
			primesReader.close();
			lowerOrderSet.close();
			higherOrderSet.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
