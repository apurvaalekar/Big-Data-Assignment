package hdfs.assignment1.partI.uploadDownload;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

public class assignment1 {

	public static void main(String[] args) {
		

		try{
		
		String[] url = new String[6];
		url[0]="http://www.utdallas.edu/~axn112530/cs6350/lab2/input/20417.txt.bz2";
		url[1]="http://www.utdallas.edu/~axn112530/cs6350/lab2/input/5000-8.txt.bz2";
		url[2]="http://www.utdallas.edu/~axn112530/cs6350/lab2/input/132.txt.bz2";
		url[3]="http://www.utdallas.edu/~axn112530/cs6350/lab2/input/1661-8.txt.bz2";
		url[4]="http://www.utdallas.edu/~axn112530/cs6350/lab2/input/972.txt.bz2";
		url[5]="http://www.utdallas.edu/~axn112530/cs6350/lab2/input/19699.txt.bz2";
		Configuration conf = new Configuration();
		
		for(int i=0;i<6;i++){
			
			String src = args[0];
			String dest=args[1];
			
			URL urll = new URL(url[i]);
			String filename=url[i].split("/")[7];
			
			
			if(src.lastIndexOf("/")==src.length()-1)
					src=src+filename;
			else
				src=src+"/"+filename;
			if(dest.lastIndexOf("/")==dest.length()-1)
				dest=dest+filename;
			else
			dest=dest+"/"+filename;
			
			
			
			
			
			File file= new File(filename);
			//downloading file into local system
			FileUtils.copyURLToFile(urll, file);
			
			//uploading to hdfs
			 InputStream in = new BufferedInputStream(new FileInputStream(filename));
			 conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
			    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));

			    FileSystem fs = FileSystem.get(URI.create(dest), conf);
			    OutputStream out = fs.create(new Path(dest), new Progressable() {
			      public void progress() {
			        System.out.print(".");
			       }
			    });
			      IOUtils.copyBytes(in, out, 4096, true);
			      System.out.println(filename + "uploading to hdfs done!" );
			      
			 //decompressing
			 decompress(dest);
			 
			 //deleting the file
			 String path_to_file=dest.split("cshadoop1")[1];
			 
			 
			 
			 Path path = new Path(path_to_file);
			 fs.delete(path,true);
			 }
					} catch (MalformedURLException e) {
		
		e.printStackTrace();
	} catch (IOException e) {
		
		e.printStackTrace();
	}
		
	}
	
	private static void decompress(String dest) {
		
		String uri = dest;
	    Configuration conf = new Configuration();
	    FileSystem fs;
		try {
			fs = FileSystem.get(URI.create(uri), conf);
		
	    
	    Path inputPath = new Path(uri);
	    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
	    CompressionCodec codec = factory.getCodec(inputPath);
	    if (codec == null) {
	      System.err.println("No codec found for " + uri);
	      System.exit(1);
	    }

	    String outputUri =
	      CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());

	    InputStream in = null;
	    OutputStream out = null;
	    try {
	      in = codec.createInputStream(fs.open(inputPath));
	      out = fs.create(new Path(outputUri));
	      IOUtils.copyBytes(in, out, conf);
	    } finally {
	      IOUtils.closeStream(in);
	      IOUtils.closeStream(out);
	    }
		} catch (IOException e) {
		
			e.printStackTrace();
		}
		
	}

}


