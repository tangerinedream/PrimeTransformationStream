#!/bin/bash
cd $DEV/eclipseProjects/PrimeTransformationStream/bin
jar cvfe ../PrimeFractal.jar com.primefractal.main.SetTransformation com
cd - >/dev/null 2>&1
echo "Don't forget to scp to AWS instance ..."
