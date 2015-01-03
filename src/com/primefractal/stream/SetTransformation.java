/**
 * 
 */
package com.primefractal.stream;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

/**
 * @author GMan
 *
 */
public class SetTransformation {
	
	protected void crunch(BufferedReader primes, BufferedReader valuesSet, BufferedWriter res) {
		System.out.println("TODO: Add File Element Count limit feature");	
		String nextPrimeIndexStr=null;
		try {
			nextPrimeIndexStr=primes.readLine();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return;
		}
		long nextPrimeIndex=new Long(nextPrimeIndexStr).longValue();
		
		boolean processComplete=false;
		String nextProcessValueStr=null;

		long processCounter=1;
		while( processComplete == false ) {
			//System.out.println("DEBUG:{"+nextPrimeIndex+", "+nextProcessValueStr+", "+processCounter+"}\n");
			
			try {
				nextProcessValueStr=valuesSet.readLine();
				if( nextProcessValueStr == null || (nextProcessValueStr.compareTo("") == 0) ) {
					processComplete=true;
					continue;
				}
				// Ensure the nextPrimeIndex to check, is >= processCounter
				// Ensure the next Prime Index is > current counter.  Must use < and not <= otherwise search for index beyond stream
				while( nextPrimeIndex < processCounter) {
					nextPrimeIndexStr=primes.readLine();
					nextPrimeIndex=new Long(nextPrimeIndexStr).longValue();
				}
				
				// if the same, this value is a member of the next set
				if( nextPrimeIndex == processCounter) {
					//System.out.println("Writing:{"+nextPrimeIndex+", "+nextProcessValueStr+", "+processCounter+"}\n");
					res.write(nextProcessValueStr);
					res.write("\n");
				}
				
				// Move the chains for the set we are processing
				processCounter++;
				
			} catch (NumberFormatException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}	
	
	public static void main(String args[]) {
		String reqSetSize=args[0];
		String kLowerOrderSetStr=args[1];
		String kHigherOrderSetStr=args[2];
		
		try {
			BufferedReader primesReader=new BufferedReader(new InputStreamReader(System.in));  // System.in is an InputStream
			
			// In the case of K=2, Why not fork the InputStream by reading a char, and writing to two different WHAT charArray ? 
			
			FileReader fReader=new FileReader("Set."+kLowerOrderSetStr+"."+reqSetSize);
			BufferedReader valuesReader=new BufferedReader(fReader);
			
			FileWriter fWriter=new FileWriter("Set."+kHigherOrderSetStr+"."+reqSetSize);
			BufferedWriter resultsWriter=new BufferedWriter(fWriter);
			System.out.println("TODO: Convert files to gzipped.");//see http://docs.oracle.com/javase/7/docs/api/java/io/OutputStreamWriter.html
			
			SetTransformation st=new SetTransformation();
			st.crunch(primesReader, valuesReader, resultsWriter);
			
			try {
				valuesReader.close();
				fReader.close();
				resultsWriter.close();
				fWriter.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
