package news.solr;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

public class ContentTitle {
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Path seqFile = new Path(args[1]);
		conf.set("io.compression.codecs", "com.hadoop.compression.lzo.LzoCodec");
		conf.set("fs.default.name", "hdfs://192.168.1.101");
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fileStatuses = fs.listStatus(seqFile);

		for (int j = 0; j < fileStatuses.length; j++) {
			SequenceFile.Reader reader = new SequenceFile.Reader(conf,
					SequenceFile.Reader.file(fileStatuses[j].getPath()));
			BufferedWriter bw = new BufferedWriter(
					new FileWriter("/mnt/lun1/data/tjubd/zz/test/".concat(String.valueOf(j + 1)).concat(".csv")));
			BytesWritable key = new BytesWritable();
			MapWritable value = new MapWritable();

//			countNews[j] = 0;
			while (reader.next(key, value)) {
//				countNews[j]++;
				Set<Writable> vs = value.keySet();// 字段集合
				for (Writable it : vs) {
					if(it.toString().equals("title")) {
						bw.write("title:"+value.get(it));
						bw.newLine();
					}else if(it.toString().equals("content")) {
						bw.write("content:"+value.get(it));
						bw.newLine();
					}
				}
			}
			bw.close();
			reader.close();
		}
	}
}
