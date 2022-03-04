cd ../
RESULT0=$(diff output0/TFICF/part-r-00000 checkoutput/tficf0.txt)
RESULT1=$(diff output1/TFICF/part-r-00000 checkoutput/tficf1.txt)

if [[ -n "$RESULT0" ]]
then
    echo "Input 0 does not match"
else
    echo "Input 0 matches"
fi

if [[ -n "$RESULT1" ]]
then
    echo "Input 1 does not match"
else
    echo "Input 1 matches"
fi