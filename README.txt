USAGE:

#Jar file is already created. To create again, change the directry to HDFSUpload and enter:
$mvn clean package

#To execute the jar file, enter:
$hadoop jar target/HDFSUpload-1.0-SNAPSHOT.jar edu.ucr.cs.cs226.aayac001.HDFSUpload <input file> <output file>

#'output file' contains the directory starting from root folder of hdfs.

eg., hadoop jar target/HDFSUpload-1.0-SNAPSHOT.jar edu.ucr.cs.cs226.aayac001.HDFSUpload /home/user/AREAWATER.csv /output


#NOTE:
The input file should contain more than 10000 bytes as the random function generates a random number from 0 to 10000.
