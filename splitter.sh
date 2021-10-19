#!/bin/bash

set -x

chunk=$1
splits=$2
reusableTestsFile=$3
renewableTestsFile=$4
outFile=$5

if [ -z "$chunk" ] || [ -z "$splits" ] || [ -z "$reusableTestsFile" ] || [ -z "$renewableTestsFile" ] || [ -z "$outFile" ]; then
  echo "Usage: splitter.sh <chunk> <splits> <reusableTestsFile> <renewableTestsFile> <outFile>"
  exit 1
fi

if [ `grep -f $reusableTestsFile $renewableTestsFile | wc -l` -gt 0 ]; then
  echo "There are tests that are in both reuse and renew files. That sould be impossible"
  exit 1
fi

# Extract test classes from reusable tests list
cat  $reusableTestsFile | sed s/[^:]*$// | sed s/::$// | uniq > reuseTestClasses.txt

awk 1 reuseTestClasses.txt > combinedTests.txt
awk 1 $renewableTestsFile >> combinedTests.txt

if [ "$CIRCLECI" = true ]; then
  circleci tests split --split-by=timings --timings-type=classname combinedTests.txt > $outFile
else
  #Split tests with round robin to spread weight of heavy test classes
  split -n r/$chunk combinedTests.txt > $outFile
fi

