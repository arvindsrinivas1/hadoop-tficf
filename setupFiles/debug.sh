cd ../
javac TFICF.java
jar cf TFICF.jar TFICF*.class
hadoop jar TFICF.jar TFICF input0 input1 &> hadoop_output.txt
rm -rf output*
hdfs dfs -get /user/asubram9/output* .