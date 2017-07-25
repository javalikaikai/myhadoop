package org.pulin.hadoop.hdfs;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

public class MyHadoop {
	
	private FileSystem fs = null;
	@Before
	public void getFs() throws IOException, InterruptedException, URISyntaxException {
		
		//get a configuration object
		Configuration conf = new Configuration();
		//to set a parameter, figure out the filesystem is hdfs
		conf.set("fs.defaultFS", "hdfs://pulin-pro:9000/");
		conf.set("dfs.replication","1");
		
		//get a instance of HDFS FileSystem Client
//		fs = FileSystem.get(conf);
		// 这种获取fs的方法可以指定访问hdfs的客户端身份
		fs = FileSystem.get(new URI("hdfs://pulin-pro:9000/"), conf, "root");
	}
	
	
	@Test
	public void Download() throws IllegalArgumentException, IOException {
		FSDataInputStream is = fs.open(new Path("hdfs://pulin-pro:9000/jdk.tar.gz"));
		
		FileOutputStream os = new FileOutputStream("/tmp/jdk.tar.gz");
		IOUtils.copy(is, os);
	}
	
		//upload a local file to hdfs
	public static void main(String[] args) throws IOException {
					
		//get a configuration object
				Configuration conf = new Configuration();
				//to set a parameter, figure out the filesystem is hdfs
				conf.set("fs.defaultFS", "hdfs://pulin-pro:9000/");
				conf.set("dfs.replication","1");
				
				//get a instance of HDFS FileSystem Client
				FileSystem fs = FileSystem.get(conf);
					
					//open a outputstream of the dest file
					Path destFile = new Path("hdfs://pulin-pro:9000/jdk.tar.gz");
					FSDataOutputStream os = fs.create(destFile);
					
					//open a inputstream of the local source file
					FileInputStream is = new FileInputStream("c:/123.txt");
					
					//write the bytes in "is" to "os"
					IOUtils.copy(is, os);
					
					
	}
}
