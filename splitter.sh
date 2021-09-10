#!/bin/bash

chunk=$1
splits=$2

if [ -z "$chunk" ] || [ -z "$splits" ]
then
  echo "Usage: splitter.sh <chunk> <splits>"
  exit 1
fi

#Get the numbers per no/reusage
linesReuse=`wc -l reuseClusterTests.txt | cut -d " " -f 1`
linesRenew=`wc -l renewClusterTests.txt | cut -d " " -f 1`
totalTests=`expr $linesReuse + $linesRenew`
testsPerSplit=`expr $totalTests / $splits`
echo "Reuse cluster tests: " $linesReuse
echo "No reuse cluster tests: " $linesRenew
echo "Total tests: " $totalTests
echo "Splits: " $splits " Chunk: " $chunk
echo "Tests per split: " $testsPerSplit

#How many splits per no/reussage of cluster?
reuseClusterSplits=`expr $linesReuse / $testsPerSplit`
renewClusterSplits=`expr $splits - $reuseClusterSplits`
echo "Reuse cluster splits: " $reuseClusterSplits
echo "Renew cluster splits: " $renewClusterSplits

#Split non reusable cluster tests with round robin to spread weight of heavy test classes
split -n r/$renewClusterSplits --suffix-length=6 --numeric-suffixes=1 --additional-suffix=testSplitsRR renewClusterTests.txt
rm renewClusterTestsRR.txt
cat x*testSplitsRR >> renewClusterTestsRR.txt

#Put reusable tests first and the RR non-reusable later. awk needed bc sometimes some file had no newline at the end
awk 1 reuseClusterTests.txt > test_list.txt
awk 1 renewClusterTestsRR.txt >> test_list.txt
rm x*testSplitsRR

split -n l/$splits --suffix-length=6 --numeric-suffixes=1 --additional-suffix=testSplits test_list.txt
printf -v formattedChunk "%06d" $chunk
mySplitFile="x${formattedChunk}testSplits"
echo "My split is: " $mySplitFile
cp $mySplitFile test_list.txt
