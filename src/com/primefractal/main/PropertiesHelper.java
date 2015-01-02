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
		this.setReqSetSize(new Long( (props.getProperty(REQ_SET_SIZE_) == null) ? "100000" : props.getProperty(REQ_SET_SIZE_)).longValue() );
		
		this.setMaxK(new Integer( (props.getProperty(MAX_K_SETS) == null) ? "5" : props.getProperty(MAX_K_SETS)).intValue() );		
				
		// set Family Name for filename purposes.  Pull from Properties and if missing use default
		this.setFamilyName( (props.getProperty(FAMILY_NAME_) == null) ? "TestFamilyMatrix" : props.getProperty(FAMILY_NAME_) );
		
		// Instead of using Stdin for stream of primes for Set K=1, use this GZip'd file instead
		this.setUseFileInputStream(new Boolean( (props.getProperty(USE_FILE_AS_INPUT_STREAM_) == null) ? "false" : props.getProperty(USE_FILE_AS_INPUT_STREAM_)).booleanValue() );

		// The subset of Primes to Realize, as we may not need/want to realize every one from a set.
		this.setMaxPrimesToRealize(new Long( (props.getProperty(MAX_PRIMES_TO_REALIZE_) == null) ? "0" : props.getProperty(MAX_PRIMES_TO_REALIZE_)).longValue() );
	}
	
	
	

	public boolean isUseFileInputStream() {
		return (useFileInputStream);
	}

	public void setUseFileInputStream(boolean useFileInputStream) {
		this.useFileInputStream = useFileInputStream;
	}

	public long getReqSetSize() {
		return (reqSetSize);
	}



	protected void setReqSetSize(long reqSetSize) {
		this.reqSetSize = reqSetSize;
	}


	public int getMaxK() {
		return (maxK);
	}

	public void setMaxK(int maxK) {
		this.maxK = maxK;
	}

	public String getFamilyName() {
		return (familyName);
	}
	
	public void setFamilyName(String familyName) {
		this.familyName = familyName;
	}

	public long getMaxPrimesToRealize() {
		return (maxPrimesToRealize);
	}

	public void setMaxPrimesToRealize(long maxPrimesToRealize) {
		this.maxPrimesToRealize = maxPrimesToRealize;
	}
	

	private final static Properties props = new Properties();
	
	private long  	reqSetSize;
	private int  	maxK;
	private boolean useFileInputStream;
	private String  familyName;
	private long	maxPrimesToRealize;
	
	final private static String REQ_SET_SIZE_="ReqSetSize";
	final private static String MAX_K_SETS="MaxK";	
	final private static String FAMILY_NAME_="FamilyName";
	final private static String MAX_PRIMES_TO_REALIZE_="MaxPrimesToRealize";
	final private static String USE_FILE_AS_INPUT_STREAM_="UseFileInputStream";
	
	public final static String FILE_DELIM_=".";
	
	final private static Logger LOGGER_=Logger.getLogger("PrimeTransformationStream");  // project wide logger
	
	private static final long serialVersionUID = -8281343888624573709L;
}
