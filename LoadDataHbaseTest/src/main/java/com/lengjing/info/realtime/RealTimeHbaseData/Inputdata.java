package com.lengjing.info.realtime.RealTimeHbaseData;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.fastjson.JSONObject;

public class Inputdata {
	public static Configuration conf=new Configuration();
	private static HConnection conn=null;
	static{
		conf.set("hbase.zookeeper.quorum", "hadoop1,hadoop2,hadoop3");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		try {
			conn=HConnectionManager.createConnection(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws IOException {
		
		File f=new File("d:\\yubing.lei\\Desktop\\aa");
		for(File list:f.listFiles()){
			//System.out.println(list.getAbsolutePath());
			File file = new File(list.getAbsolutePath());
	        BufferedReader reader = null;
	        try {
	            //System.out.println("以行为单位读取文件内容，一次读一整行：");
	            reader = new BufferedReader(new FileReader(file));
	            String tempString = null;
	            int line = 1;
	            // 一次读入一行，直到读入null为文件结束
	            while ((tempString = reader.readLine()) != null) {
	            	JSONObject jsonobj=JSONObject.parseObject(tempString);
	    			String companyname=jsonobj.getString("inputCompanyName").trim();
	    			System.out.println(companyname);
	                
	            }
	            reader.close();
	        } catch (IOException e) {
	            e.printStackTrace();
	        } finally {
	            if (reader != null) {
	                try {
	                    reader.close();
	                } catch (IOException e1) {
	                }
	            }
	        }
		}
	
	}
	
}
