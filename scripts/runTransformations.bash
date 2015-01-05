#!/bin/bash

export JVM_MAX_HEAP="10G"

export TRANSFORMATION_CNT="15" 
#export SET_K_1_SIZE="$((10**9))"
export SET_K_1_SIZE=50000000
export DEFAULT_SET_1_NAME="Set.1.${SET_K_1_SIZE}.gz"

echo TRANSFORMATION_CNT = $TRANSFORMATION_CNT
echo SET_K_1_SIZE = $SET_K_1_SIZE
echo SET_1_NAME = $DEFAULT_SET_1_NAME

# Create the K=1 Set if it does not exist
if [ ! -f ${DEFAULT_SET_1_NAME} ]; then
	echo "Set 1 does not exist, creating"
	primesieve ${SET_K_1_SIZE} --print | gzip > ${DEFAULT_SET_1_NAME}
fi

# Perform the Transformations
export LOOP_END=$((TRANSFORMATION_CNT-1))
for i in $(seq 1 $LOOP_END)
do
	export K_LOW_ORDER_SET=$i
	export K_HIGH_ORDER_SET=$((i+1))
	echo "Performing Transformation for Set K=$K_LOW_ORDER_SET to Set K=$K_HIGH_ORDER_SET"
###
# Use this command when you want to stream in 
#	primesieve ${SET_K_1_SIZE} --print | java -Xmx${JVM_MAX_HEAP} com.primefractal.stream.SetTransformation ${SET_K_1_SIZE} ${K_LOW_ORDER_SET} $((K_HIGH_ORDER_SET))
###

###
# Use this command when you have configured in PrimeTransformationStream.props to "UseFileInputStream=true"
#     in this case, it will use the generated file for Set 1
	time java -Xmx${JVM_MAX_HEAP} com.primefractal.stream.SetTransformation ${SET_K_1_SIZE} ${K_LOW_ORDER_SET} $((K_HIGH_ORDER_SET))
#

done

echo "NOTE: If you ran a truncated resutls set, now is a good time to run 'primesieve <truncated set size> --print | gzip > Set.1.xxx.gz' to overwrite the potentially huge original Set K=1"
