source hadoop-setup.sh &> setup_output.txt

RESULT=$(hdfs dfs -ls /user)

echo $RESULT