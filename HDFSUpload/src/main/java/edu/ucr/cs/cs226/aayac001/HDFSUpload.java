package edu.ucr.cs.cs226.aayac001;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class HDFSUpload {

		public static void main(String[] args) throws IOException{
			
			String local = args[0];
			String hdfs = "hdfs://localhost:9000" + args[1];
			
			Configuration conf = new Configuration();
			
			if(copyFile(conf, local, hdfs))
				if(readFile(conf, hdfs))
					randomAccess(conf, hdfs);
		}
		
		public static boolean copyFile(Configuration conf, String input, String hdfs) throws IOException{
			System.out.println("");
			
			long startTime, endTime, totalTime;
			
			FileSystem fs = null;
			FSDataOutputStream out = null;
			InputStream in = null;
	
			try {
			  File inputFile = new File(input);
			  if(inputFile.exists()){
				   Path outputPath = new Path(hdfs);
				   fs = outputPath.getFileSystem(conf);
				
				   if(!fs.exists(outputPath)){
					    System.out.println("Writing File in HDFS...");
					    out = fs.create(outputPath);
					    in = new BufferedInputStream(new FileInputStream(inputFile));
					    
					    byte[] b = new byte[1024];				
					    int check = 0;
					    startTime = System.nanoTime();
					    while ((check = in.read(b)) > 0)
					    	out.write(b);
					    endTime   = System.nanoTime();
					    totalTime = endTime - startTime;
				   }else{
					    System.out.println("ERROR: File already exists in HDFS");
					    return false;
				   }
			
				   if(fs.exists(outputPath)) {
					    System.out.println("File copied in HDFS");
					    System.out.println("Total time taken to copy the file: " + totalTime/1000000000 + " sec.");
					    System.out.println();
				   }
				   
			  }else{
				  System.out.println("ERROR: Source file does not exist");
				  return false;
			  }
			 }catch(Exception e){
				 System.out.println(e);
			 }finally{			
				  if (in != null)
				   in.close();
				  if (out != null)
				   out.close();
				  if (fs != null)
				   fs.close();
			 }
			return true;
		}
		
	
		public static boolean readFile(Configuration conf, String hdfs) throws IOException{
			
			long startTime, endTime, totalTime;
			
			FileSystem fs = FileSystem.get(conf); 
			FSDataInputStream in = fs.open(new Path(hdfs));
	        byte[] b = new byte[1024];
	        System.out.println("Reading File...");
	        startTime = System.nanoTime();
	        while (in.read(b) > 0) {
	            //read file
	        }
	        endTime   = System.nanoTime();
	        totalTime = endTime - startTime;
	        System.out.println("Total time taken to read the file: " + totalTime/1000000000 + " sec.");
	        System.out.println();
	       
	        if (in != null)
			   in.close();
	        if (fs != null)
			   fs.close();	
	        
	        return true;
		}

		public static boolean randomAccess(Configuration conf, String hdfs) throws IOException{
			
			long startTime, endTime, totalTime;
			
		    Random random = new Random();
		    FileSystem fs = FileSystem.get(conf);
		    FSDataInputStream in = fs.open(new Path(hdfs));
	        byte[] b = new byte[1024];
	        System.out.println("Randomly Accessing 2000 1 KB files...");
	        startTime = System.nanoTime();
	        for (int i = 0; i < 2000; i++) {
	        	long rand = (long) random.nextInt(10000);
	            in.seek(rand);
	            in.read(b);
	        }
	        endTime = System.nanoTime();
	        
	        totalTime = endTime-startTime;
	        System.out.println("Total time taken to make 2000 random accesses of size 1KB: " + totalTime/1000000000 + " sec.");
	        System.out.println();
	        
	        if (in != null)
			   in.close();
		    if (fs != null)
			   fs.close();
			
		    return true;
		    
		}
		
}		


