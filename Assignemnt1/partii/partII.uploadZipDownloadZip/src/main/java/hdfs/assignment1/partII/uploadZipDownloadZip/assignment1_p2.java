package hdfs.assignment1.partII.uploadZipDownloadZip;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;


public class assignment1_p2 {

	public static void main(String[] args) {
		

		try{
			
			String src = args[0];
			String dest=args[1];

			Configuration conf = new Configuration();
			
			String url="http://corpus.byu.edu/wikitext-samples/text.zip";
				
				
				URL urll = new URL(url);
				String filename=url.split("/")[4];
				
				if(src.lastIndexOf("/")==src.length()-1)
						src=src+filename;
				else
					src=src+"/"+filename;
				File file= new File(filename);
				String destination =dest ;
 				
				//downloading file into local system
				FileUtils.copyURLToFile(urll, file);
				
				if(dest.lastIndexOf("/")==dest.length()-1)
					dest=dest+filename;
				else
				dest=dest+"/"+filename;
				
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
				 System.out.println("Uplading to hdfs done!");
				 
				 //unzipping 
				 unzip(destination,filename);
				 
				//deleting the file
				String path_to_file=dest.split("cshadoop1")[1];
				Path path = new Path(path_to_file);
			    fs.delete(path,true);
				 
			}catch (MalformedURLException e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}
	private static void unzip(String dir, String file) {
	 	  byte[] buffer = new byte[4096];
		   try {

		      FileInputStream fin = new FileInputStream(file);
		      ZipInputStream zipInput = new ZipInputStream(fin);
		      ZipEntry entry = zipInput.getNextEntry();

		      while(entry != null){

		          String entryName = entry.getName();

		          String file_string = dir + "/" + entryName ;
		       	  Configuration conf = new Configuration();
		      	  conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
			      conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));

				  FileSystem fs = FileSystem.get(URI.create(file_string), conf);
				  OutputStream out = fs.create(new Path(file_string), new Progressable() {
			      public void progress() {
				        System.out.print(".");
			       }
			    });
		        	  
			   int count = 0;
		       while ((count = zipInput.read(buffer)) > 0) {
		            	  out.write(buffer, 0, count);
		       }
		       out.close();
               zipInput.closeEntry();
    	       entry = zipInput.getNextEntry();

		      }
		      zipInput.closeEntry();
		      zipInput.close();

		      fin.close();

		  } catch (IOException e) {

		      e.printStackTrace();

		  }

	}
}

