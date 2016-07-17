package BigDataAssignment1.Part1;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;


public class FileDecompressorAndDeleter {

	  public static void main(String[] args) throws Exception {
	    String uri = args[0]+"/";
	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(URI.create(uri), conf);
	    FileStatus[] allFiles = fs.listStatus(new Path(uri));
	    
	    	String currentFile = allFiles[0].getPath().toString();
	    	System.out.println(currentFile);
		    Path inputPath = new Path(currentFile);
		    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		    CompressionCodec codec = factory.getCodec(inputPath);
		    if (codec == null) {
		      System.err.println("No codec found for " + uri);
		      System.exit(1);
		    }
	
		    String outputUri =
		      CompressionCodecFactory.removeSuffix(currentFile, codec.getDefaultExtension());
	
		    InputStream in = null;
		    OutputStream out = null;
		    try {
		      in = codec.createInputStream(fs.open(inputPath));
		      out = fs.create(new Path(outputUri));
		      IOUtils.copyBytes(in, out, conf);
		    } finally {
		    	fs.delete(inputPath, true);
		    	IOUtils.closeStream(in);
		    	IOUtils.closeStream(out);
		    }
	    
	    
	    for(int i = 1; i < allFiles.length; i++){
	    	currentFile = allFiles[i].getPath().toString();
	    	System.out.println(currentFile);
		    inputPath = new Path(currentFile);
		    factory = new CompressionCodecFactory(conf);
		    codec = factory.getCodec(inputPath);
		    if (codec == null) {
		      System.err.println("No codec found for " + uri);
		      System.exit(1);
		    }
	
		    outputUri =
		      CompressionCodecFactory.removeSuffix(currentFile, codec.getDefaultExtension());
		    try {
		      in = codec.createInputStream(fs.open(inputPath));
		      out = fs.create(new Path(outputUri));
		      IOUtils.copyBytes(in, out, conf);
		    } finally {
		    	fs.delete(inputPath, true);
		    	IOUtils.closeStream(in);
		    	IOUtils.closeStream(out);
		    }
	    }
	  }
	}
	// ^^ FileDecompressor

