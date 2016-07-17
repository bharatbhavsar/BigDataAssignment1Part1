package BigDataAssignment1.Part1;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;



public class FileDownloaderToLocal {
 
	static FileDownloaderToLocal obj = new FileDownloaderToLocal();
	static HashSet<String> myUrls = new HashSet<String>(); 
	static String filename;
	static String myPath;
	static String userID;
	static String timeStamp;
	
	public static void main(String[] args) throws IOException{
		if(args.length != 1){
			System.out.println("Please enter Filename!");
		}else{
		 
			filename = args[0];
			//userID = args[1];
			//Open file
			//Parse file and save each line as URL to HashSet
			FileReader fr = new FileReader(filename);
			BufferedReader br = new BufferedReader(fr);
			String currentLine;
			
			while(( currentLine = br.readLine( ) ) != null){
				myUrls.add(currentLine);
			}
			
			for(String c : myUrls){
				System.out.println(c);
			}
			
			
			
			
			
			//Use this HashSet to download each URL to local file
			obj.fileDownloader();
			
			
		}	
	}
	
	void fileDownloader() throws IOException{
		
		String s = new File(".").getAbsolutePath().toString();
		timeStamp = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
		
	    myPath = s.toString()+"/" + timeStamp;
	    
	    System.out.println(myPath);
	    new File(myPath).mkdir();
	    
	    int i=0;
	    for(String thiUrl : myUrls){
		    URL url;
			try {
				url = new URL(thiUrl);
				HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			    connection.setRequestMethod("GET");
			    InputStream in = connection.getInputStream();
			    FileOutputStream out = new FileOutputStream(myPath + "/download"+i+".bz2");
			    copy(in, out, 4096);
			    out.close();
			    i++;
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	}
	
	
	
	public static void copy(InputStream input, OutputStream output, int bufferSize) throws IOException {
        byte[] buf = new byte[bufferSize];
        int n = input.read(buf);
        while (n >= 0) {
          output.write(buf, 0, n);
          n = input.read(buf);
        }
        output.flush();
      }
}
