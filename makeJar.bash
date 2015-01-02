#!/bin/bash
cd $DEV/eclipseProjects/PrimeTransformationStream/bin
jar cvfe ../PrimeFractal.jar com.primefractal.main.Driver com
cd -
echo "Don't forget to scp to AWS instance ..."
