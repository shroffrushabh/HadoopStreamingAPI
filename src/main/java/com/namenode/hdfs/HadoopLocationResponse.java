package com.namenode.hdfs;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopLocationResponse {

	public static String location;
	private static Configuration conf;	
	private static FileSystem fs;
	private static String NameNodeLocation="hdfs://172.16.17.144:54310";
	
	HadoopLocationResponse() throws IOException{
		 conf = new Configuration();
		 conf.set("fs.default.name", NameNodeLocation);
		 fs= FileSystem.get(conf);
	}
	
	public static ResponseObject getLocationOfBlock(String dir, String fileName) throws IOException {

		Path srcPath = new Path(NameNodeLocation+"/usr/"+dir+"/"+fileName);
		FileStatus fileStatus = fs.getFileStatus(srcPath);
		BlockLocation [] locations = fs.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());
		
		HashSet setOfLocations = new HashSet();
		for(int i=0;i<locations.length;i++){
			String temp[] = locations[i].getHosts();
			for(int j=0;j<temp.length;j++){
				location = temp[j];
			}
		}

		ResponseObject resp = new ResponseObject();
		resp.setLocation(location);
		return resp;
	}	
	
}
