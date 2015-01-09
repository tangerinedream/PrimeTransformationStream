# PrimeTransformationStream
Prime-Index Set Transformations for Prime Numbers

# Goal of project
Consume a (large) stream of prime numbers, and generate "k" numbers of Set Transformations on that stream, saving each transformed set to a compressed file. A large stream of primes is on the order of all primes between 1 and 10^^15th.

Set Transformation is performed by reading two input sources (e.g. streams) and producing one output stream (higher order set).  The first input stream is the set of Prime numbers.  The second stream is the Set of numbers to be transformed (lower order set).  Members from the lower order set are promoted to the higher order set if the set index of the element is a prime number (hence the requirement for the first input stream for every transformation).

# Current Status
Partially completed.  Transformation is working using input files for the two input streams. SetTransformation has been proven for Set K=0 with 10^^13 elements.  Running time was 12 hours on AWS r3.xlarge.

Full stream input development is in progress.

# Usage
nohup java com.primefractal.stream.SetTransformation <number of elements in set to be used to generate primes (e.g. 10^^12)> &

Ensure the configuration file PrimeTransformationStream.props is accessible via CLASSPATH

# Configuration
Have a look into the scripts folder for some basic scripts 
run makeJar.bash from the parent directory of src
export CLASSPATH=<path to jar file>
Be sure to edit the PrimeTransformationStream.props
