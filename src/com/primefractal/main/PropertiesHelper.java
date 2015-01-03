/**
 * 
 */
package com.primefractal.main;

import java.io.IOException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * @author GMan
 *
 */
final public class PropertiesHelper extends Properties {


	private static PropertiesHelper instance = null;


	
    synchronized public static PropertiesHelper getInstance() {
        if(instance == null) {
            instance = new PropertiesHelper();
            instance.loadProperties();
         }
         return instance;
    }
	
	private void loadProperties() {
		// Load the Properties file
		try {
			props.load(Driver.class.getClassLoader().getResourceAsStream("PrimeTransformationStream.props"));
		} catch (IOException e) {
			LOGGER_.severe("No Properties File found.");
			e.printStackTrace();
			return;
		}	
		
		
		// Yes this is ugly, which is why it is isolated and centralized to this class.  The readability of the callers of this class is vastly improved.
		
		// This setting limits the size of each result file, to include only this number of elements.  Zero mean do not limit the elem count
		this.setFixedElementCountInResultSet(new Long( (props.getProperty(FIXED_ELEM_CNT_IN_RES_SET_) == null) ? "0" : props.getProperty(FIXED_ELEM_CNT_IN_RES_SET_)).longValue() );
		
		this.setMaxK(new Integer( (props.getProperty(MAX_K_SETS) == null) ? "5" : props.getProperty(MAX_K_SETS)).intValue() );		
				
		// set Family Name for filename purposes.  Pull from Properties and if missing use default
		this.setFamilyName( (props.getProperty(FAMILY_NAME_) == null) ? "TestFamilyMatrix" : props.getProperty(FAMILY_NAME_) );
		
		// Instead of using Stdin for stream of primes for Set K=1, use this GZip'd file instead
		this.setUseFileInputStream(new Boolean( (props.getProperty(USE_FILE_AS_INPUT_STREAM_) == null) ? "false" : props.getProperty(USE_FILE_AS_INPUT_STREAM_)).booleanValue() );

		// To be read in as command line argument for better scripting integration
//		this.setSizeOfIntegerSet(new Long( (props.getProperty(SIZE_OF_INTEGER_SET_) == null) ? "100000" : props.getProperty(SIZE_OF_INTEGER_SET_)).longValue() );

	}
	
	
	

	public boolean isUseFileInputStream() {
		return (useFileInputStream);
	}

	public void setUseFileInputStream(boolean useFileInputStream) {
		this.useFileInputStream = useFileInputStream;
	}

	public long getSizeOfIntegerSet() {
		return (sizeOfIntegerSet);
	}



	public void setSizeOfIntegerSet(long size) {
		this.sizeOfIntegerSet = size;
	}
	
	


	public long getFixedElementCountInResultSet() {
		return (FixedElementCountInResultSet);
	}

	protected void setFixedElementCountInResultSet(long fixedElementCountInResultSet) {
		FixedElementCountInResultSet = fixedElementCountInResultSet;
	}

	public int getMaxK() {
		return (maxK);
	}

	protected void setMaxK(int maxK) {
		this.maxK = maxK;
	}

	public String getFamilyName() {
		return (familyName);
	}
	
	protected void setFamilyName(String familyName) {
		this.familyName = familyName;
	}

	

	private final static Properties props = new Properties();
	
	private long  	sizeOfIntegerSet;
	private long	FixedElementCountInResultSet;
	private int  	maxK;
	private boolean useFileInputStream;
	private String  familyName;

	
	//final private static String SIZE_OF_INTEGER_SET_="SizeOfIntegerSet";  // read in as args command line param, not Props file, but still managed by this class
	final private static String FIXED_ELEM_CNT_IN_RES_SET_="FixedElementCountInResultSet";
	final private static String MAX_K_SETS="MaxK";	
	final private static String FAMILY_NAME_="FamilyName";
	final private static String USE_FILE_AS_INPUT_STREAM_="UseFileInputStream";
	
	public final static String FILE_DELIM_=".";
	
	
	public final static int		PRIMES_Q_BUF_SIZE_	=1000000;	// 10^6
	public final static int		HIGH_Q_BUF_SIZE_	=1000000;	// 10^6
	public final static int		Q_OFFER_SLEEP_DURATION_ = 50; // millis
	public final static int		Q_POLL_SLEEP_DURATION_ 	= 50; // millis
	public final static Long 	EOF_FOR_QUEUE_=new Long(-1);
	
	final private static Logger LOGGER_=Logger.getLogger("PrimeTransformationStream");  // project wide logger
	
	private static final long serialVersionUID = -8281343888624573709L;
}
