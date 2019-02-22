package news.solr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;

import java.io.*;
import java.util.*;
import org.apache.log4j.Logger;

/**
 * Edited by zz on 2018/12/3.
 */
public class SwitchSequence {
	public static final Logger logger = Logger.getLogger("logger.debug");

	public static void main(String[] args) throws Exception {
		SwitchSequence ssq = new SwitchSequence(args);
		ssq.extractKeys();
		// ssq.saveKeys();
	}

	Configuration conf;
	FileSystem filesystem;
	Path path;
	HashMap<String, Integer> keysets;
	Integer cnt = 0;

	SwitchSequence(String[] args) throws Exception {
		conf = new Configuration();
		conf.set("io.compression.codecs", "com.hadoop.compression.lzo.LzoCodec");
		conf.set("fs.default.name", "hdfs://192.168.1.101");
		path = new Path(args[1]);
		filesystem = FileSystem.get(conf);
	}

	public void extractKeys() throws Exception {
		RemoteIterator<LocatedFileStatus> fs = filesystem.listFiles(path, true);
		while (fs.hasNext()) {
			try {
				LocatedFileStatus localFileStatus = fs.next();
				System.out.println(localFileStatus.getPath().toString() + ";");

				BytesWritable key = new BytesWritable();
				MapWritable value = new MapWritable();
				SequenceFile.Reader reader = new SequenceFile.Reader(conf,
						SequenceFile.Reader.file(localFileStatus.getPath()));

				// 解压并写入新文件中
				File localPath = new File("./".concat(localFileStatus.getPath().getParent().toString().substring(21)));
				if (!localPath.exists()) {
					localPath.mkdirs();
					System.out.println("Create folder: " + localPath.toString());
				}
				PrintWriter bw = new PrintWriter(new FileWriter(
						"./".concat(localFileStatus.getPath().toString().substring(21)).concat(".zz.txt")));
				while (reader.next(key, value)) {
					bw.println("&*%*zz*%*&");
					Set<Writable> vs = value.keySet();// 字段集合
					for (Writable it : vs) {
						bw.println(it.toString() + ":" + value.get(it));
					}
				}
				bw.close();
				reader.close();
			} catch (Exception e) {
				logger.error("Error: ", e);
			}
		}
	}
}
