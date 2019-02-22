package tjubd.news.wang;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;

import java.io.*;
import java.util.*;
import org.apache.log4j.Logger;  
/**
 * Created by wangshuai on 2018/12/3.
 */
public class SwitchSequence {
	public static final Logger logger = Logger.getLogger("logger.debug"); 
	public static void main(String[] args) throws Exception {
		SwitchSequence ssq=new SwitchSequence(args);
		ssq.extractKeys();
		//ssq.saveKeys();
	}
	Configuration conf;
	FileSystem filesystem;
	Path path;
	HashMap<String,Integer> keysets;
	Integer cnt=0;
	SwitchSequence(String[] args) throws Exception
	{

		conf = new Configuration();
		conf.set("io.compression.codecs", "com.hadoop.compression.lzo.LzoCodec");
		conf.set("fs.default.name", "hdfs://192.168.1.101");
		path = new Path(args[1]);
		filesystem = FileSystem.get(conf);	
		keysets = new HashMap<String,Integer>();
	}
	
	public void extractKeys() throws Exception
	{
		RemoteIterator<LocatedFileStatus> fs=filesystem.listFiles(path, true);
		while(fs.hasNext())
		{
			try
			{
				LocatedFileStatus localFileStatus = fs.next();
				System.out.println(localFileStatus.getPath().toString()+"; Keyset size: "+ keysets.size());
					
				HashMap<String,Integer> keyset = new HashMap<String,Integer>();
				BytesWritable key = new BytesWritable();
				MapWritable value = new MapWritable();
				SequenceFile.Reader reader = new SequenceFile.Reader(conf,
						SequenceFile.Reader.file(localFileStatus.getPath()));
				Integer icnt=0;
				while (reader.next(key, value)) {
					Set<Writable> vs = value.keySet();// 字段集合
					for (Writable it : vs) {
						icnt++;
						if(keysets.containsKey(it.toString()))
							keysets.put(it.toString(), keysets.get(it.toString())+1);
						else
							keysets.put(it.toString(), 1);
						
						if(keyset.containsKey(it.toString()))
							keyset.put(it.toString(), keyset.get(it.toString())+1);
						else
							keyset.put(it.toString(), 1);
					}
				}
				reader.close();
				cnt+=icnt;
				saveKeys();
				
				File localPath = new File("./".concat(localFileStatus.getPath().getParent().toString().substring(21)));
				if(!localPath.exists())
				{
					localPath.mkdirs();
					System.out.println("Create folder: "+localPath.toString());
				}
				BufferedWriter bw = new BufferedWriter(new FileWriter("./".concat(localFileStatus.getPath().toString().substring(21)).concat(".count.csv")));
				Iterator iter = keyset.entrySet().iterator();
				bw.write(icnt.toString());
				bw.newLine();
				while(iter.hasNext()) {
					bw.write(iter.next().toString());
					bw.newLine();
				}				
				bw.close();	
			}
			catch(Exception e)
			{
				logger.error("Error: ", e);
			}		
		}
		
	}
	
	public void saveKeys() throws IOException
	{
		BufferedWriter bw = new BufferedWriter(new FileWriter( "./keyscount.csv"));
		Iterator iter = keysets.entrySet().iterator();
		bw.write(cnt.toString());
		bw.newLine();
		while(iter.hasNext()) {
			bw.write(iter.next().toString());
			bw.newLine();
		}
		
		bw.close();		
	}	
}
