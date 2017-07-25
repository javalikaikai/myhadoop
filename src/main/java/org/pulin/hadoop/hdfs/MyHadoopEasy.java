package org.pulin.hadoop.hdfs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

public class MyHadoopEasy {
	
	private FileSystem fs = null;
	@Before
	public void getFs() throws Exception {
		
		//鎷垮埌涓�涓厤缃弬鏁扮殑灏佽瀵硅薄锛屾瀯閫犲嚱鏁颁腑灏变細瀵筩lasspath涓嬬殑xxx-site.xml鏂囦欢杩涜瑙ｆ瀽
		// 鐪熷疄鐨勯」鐩伐绋嬩腑灏卞簲璇ユ妸xxx-site.xml鏂囦欢鍔犲叆鍒板伐绋嬩腑鏉�
		Configuration conf = new Configuration();
		//to set a parameter, figure out the filesystem is hdfs
		conf.set("fs.defaultFS", "hdfs://pulin-pro:9000/");
		conf.set("dfs.replication","1");
		
		//鑾峰彇鍒颁竴涓叿浣撶殑鏂囦欢绯荤粺鐨勫鎴风瀹炰緥瀵硅薄锛屼骇鐢熺殑绀轰緥绌剁珶鏄�
		// 鍝竴绉嶆枃浠剁郴缁熺殑瀹㈡埛绔紝鏄牴鎹甤onf涓殑鐩稿叧鐨勫弬鏁版潵鍐冲畾鐨�
//		fs = FileSystem.get(conf);
		fs = FileSystem.get(new URI("hdfs://pulin-pro:9000/"), conf, "root");
	}
	/**
	 * 鏂囦欢涓婁紶
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void uploadTest() throws IllegalArgumentException, IOException {
		
		fs.copyFromLocalFile( new Path("C:\\新建文本文档 (2).txt"),new Path("/新建文本文档 (2).txt"));
	}
	
	/**
	 * 鍒犻櫎鏂囦欢
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testRmFile() throws IllegalArgumentException, IOException {
		
		boolean res = fs.delete(new Path("/新建文本文档 (2).txt"),true);
		System.out.println(res?"delete succcessfully ":"delete failed");
	}
	/**
	 * 鍒涘缓鏂囦欢澶�
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testMakeDir() throws IllegalArgumentException, IOException {
		boolean mkdirs = fs.mkdirs(new Path("/cc/dd"));
		
	}
	/**
	 * 閲嶅懡鍚嶆枃浠跺す
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testModi() throws IllegalArgumentException, IOException {
		fs.rename(new Path("/cc"), new Path("/1234"));
	}
	/**
	 * 鍒楀嚭鐩綍涓嬬殑鏂囦欢淇℃伅
	 * @throws FileNotFoundException
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testListFiles() throws FileNotFoundException, IllegalArgumentException, IOException {
		// 閫掑綊鍒楀嚭鏂囦欢
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		
		
		while(listFiles.hasNext()) {
			LocatedFileStatus next = listFiles.next();
			System.out.println(next.getPath().getName());
		}
		System.out.println("=========================");
		//鍒楀嚭鏂囦欢鍙婃枃浠跺す
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for (int i = 0; i < listStatus.length; i++) {
			
			System.out.println(listStatus[i].getPath().getName()+"         "+ (listStatus[i].isDirectory() ? "d":"f"));
		}
//		for(FileStatus file:listStatus) {
//			System.out.println(file.getPath().getName()+"         "+ (file.isDirectory()?"d":"f"));
//		}
		
	}
	/**
	 * 浠巋dfs涓嬭浇鏁版嵁鍒版湰鍦�
	 * @throws IllegalArgumentException
	 * @throws IOException
	 */
	@Test
	public void testDownLoad() throws IllegalArgumentException, IOException {
		fs.copyToLocalFile(new Path("/hadoop-root-datanode.pid"), new Path("C:/hadoop-root-datanode.pid"));
	}
	
	
}
