/**
 * 
 */
package com.primefractal.utils;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import com.primefractal.main.PropertiesHelper;

/**
 * @author GMan
 *
 */
public class IOUtils {

	public static Long getLongFromFile(Reader file) {
		StringBuffer resultSB=null;
		try {
			resultSB=new StringBuffer("");
			
			int charRead=file.read();
			while( ((char)charRead != (char)EOF_INT_) && ((char)charRead != NEWLINE_CHAR_) ) {
				resultSB.append((char)charRead);
				charRead=file.read();
			}
			if( ((char)charRead == (char)EOF_INT_) ) {
				return(QueueUtils.EOF_FOR_QUEUE_);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return(new Long(resultSB.toString()));
	}
	
	public static Reader makeSetOneReaderWithDefaultFilename() {
		long sizeOfIntegerSet=PropertiesHelper.getInstance().getSizeOfIntegerSet();
		String setPrefix=PropertiesHelper.getInstance().getFamilyName();
		String filename=setPrefix+DELIM_+"1"+DELIM_+sizeOfIntegerSet+GZIP_SUFFIX_;
		return( makeReader(filename) );
	}
	
	public static Reader makeSetOneReaderFromStdin() {
		// System.in is an InputStream
		BufferedReader stdinReader=new BufferedReader(new InputStreamReader(System.in)); 
		return(stdinReader);
	}
	
	public static Reader makeInputSetReaderWithDefaultFilename(long kLowerOrderSetNbr) {
		long sizeOfIntegerSet=PropertiesHelper.getInstance().getSizeOfIntegerSet();
		String setPrefix=PropertiesHelper.getInstance().getFamilyName();
		String filename=setPrefix+DELIM_+kLowerOrderSetNbr+DELIM_+sizeOfIntegerSet+GZIP_SUFFIX_;
		return( makeReader(filename) );
	}
	
	public static Writer makeProcessedFileWriterWithDefaultFilename(long kHigherOrderSetNbr) {
		long sizeOfIntegerSet=PropertiesHelper.getInstance().getSizeOfIntegerSet();
		String setPrefix=PropertiesHelper.getInstance().getFamilyName();
		String filename=setPrefix+DELIM_+kHigherOrderSetNbr+DELIM_+sizeOfIntegerSet+GZIP_SUFFIX_;
		OutputStreamWriter resultsFile=null;		
		try {
			// 2. Open the FileOutputStream
			FileOutputStream fos=new FileOutputStream(filename);
			BufferedOutputStream bos=new BufferedOutputStream(fos);
			
			// 3. Wrap fos into GZIP Filter Output Stream
			GZIPOutputStream gOut=new GZIPOutputStream(bos);
			
			// 4. Wrap gOut stream with BitOutputStream so we can stream booleans
			resultsFile=new OutputStreamWriter(gOut);
			
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return(resultsFile);
	}
	
	private static Reader makeReader(String filename) {
		// Convert to Reader
		InputStreamReader isr=null;
		try {
			FileInputStream fis=new FileInputStream(filename);
			// Buffer for performance 
			BufferedInputStream bis=new BufferedInputStream(fis);
			// Gunzip
			GZIPInputStream gis=new GZIPInputStream(bis);
			isr = new InputStreamReader(gis);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return(isr);	
	}
	
	//private final static String 	SET_PREFIX_="Set";
	private final static String 	GZIP_SUFFIX_=".gz";
	private final static String 	DELIM_=".";
	public 	final static int		EOF_INT_=-1;
	public 	final static long		EOF_LONG_=new Long(-1);
	private final static char		NEWLINE_CHAR_='\n';
	public final static String		NEWLINE_STR_="\n";
}
