/*Megha Krishnamurthy
800974844 */

1)Download and Install Cloudera

2) Create build folder in the home directory

3) Follow each instruction below to the compile and run the java files

4) Create required input file directories as below
	
hadoop fs -mkdir /user/cloudera/pagerank
        
hadoop fs -mkdir /user/cloudera/pagerank/input

5) You can read the output of the each file in the terminal like below
	
hadoop fs -cat /user/cloudera/pagerank/output/*

6) For extracting the file
	
hadoop fs -get /user/cloudera/pagerank/output/part-r-00000 /home/pagerank.out

Running Pagerank.java to calculate the pagerank of wikipedia page

//compiling java file

javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* PageRank.java -d build -Xlint

//compress to jar

jar -cvf PageRank.jar -C build/ .

//run mapreduce
hadoop jar PageRank.jar org.myorg.PageRank /user/cloudera/pagerank/input /user/cloudera/pagerank/output


-> Output will be produced in the arg[1]
-> Noise is removed by not considering the nodes which  doest not have url list
