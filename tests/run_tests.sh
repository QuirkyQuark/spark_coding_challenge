#!/bin/bash -ex

directories=()
for dir in $(ls -d */)
do
    directories+=($dir)
done

results=()
#run tests
for directory in ${directories[@]}
do
    #copy input and expected files
    cp $directory/input.txt ..
    cp $directory/expected.txt ..
    
    pushd ..
    spark-submit validate_ipv4.py
    if numdiff expected.txt output.txt
    then
	results+=("passed")
    else
	results+=("failed")
    fi
    
    #clean up
    rm input.txt
    rm expected.txt
    rm output.txt
    
    popd
done

set +x

#print out test results
for((i=0;i<${#directories[@]};i++))
do
    test=${directories[i]}
    test=${test%/}
    test=${test#test}

    result=${results[i]}

    echo "result for test $test: $result"
done
