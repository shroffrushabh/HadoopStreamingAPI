package com.namenode.hdfs;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.DateFormat;
import java.util.Date;
import java.util.Locale;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

/**
 * Handles requests for the application home page.
 */
@Controller
public class HomeController {
		
	public static String location;
	private static Configuration conf;	
	private static FileSystem fs;
	private static String NameNodeLocation="hdfs://172.16.17.147:54310";
	private static final Logger logger = LoggerFactory.getLogger(HomeController.class);

	HomeController() throws IOException{
		 conf = new Configuration();
		 conf.set("fs.default.name", NameNodeLocation);
		 fs= FileSystem.get(conf);
	}

	@RequestMapping(method = RequestMethod.GET, value="/download")
	public void doDownload(HttpServletRequest request,
			HttpServletResponse response) throws Exception  {

		String dir = request.getParameter("dir");
		String fileName = request.getParameter("fileName");
		Path path = new Path(NameNodeLocation+"/usr/"+dir+"/"+fileName);
	    if (!fs.exists(path)) {
	        System.out.println("File does not exists: "+path.toString());
	        return; 
	    }

	    FSDataInputStream in = fs.open(path);
	    
		String mimeType = mimeType = "application/octet-stream";
		response.setContentType(mimeType);
		OutputStream outStream = response.getOutputStream();

	    byte[] b = new byte[1024];
	    int numBytes = 0;
	    while ((numBytes = in.read(b)) > 0) {
	    	outStream.write(b, 0, b.length);
	    }
	}
	
	
}
