package tjubd.news;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;

public class MyThread extends Thread {
	// 连接Hadoop文件系统
	private Configuration conf;
	private FileSystem filesystem;
	private Path filePath;// Hadoop文件系统的路径
	private int countMonth = 0;// 一个月总体新闻计数

	public MyThread() {
	}

	// 带参构造
	public MyThread(String name, Path filePath) {
		super(name);
		this.filePath = filePath;
		// 数据库信息配置
		conf = new Configuration();
		conf.set("io.compression.codecs", "com.hadoop.compression.lzo.LzoCodec");
		conf.set("fs.default.name", "hdfs://192.168.1.101");
		try {
			filesystem = FileSystem.get(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		try {
			RemoteIterator<LocatedFileStatus> fs = filesystem.listFiles(filePath, true);
			PrintWriter countF = new PrintWriter(new FileWriter(this.getName() + ".Count.txt"));// Jan.Count.txt
			while (fs.hasNext()) {
				int count = 0;// 每个文件新闻计数
				LocatedFileStatus localFileStatus = fs.next();
				if (localFileStatus.getPath().toString().endsWith(".del")) {
					continue;
				}
				System.out.println(localFileStatus.getPath().toString() + ";");// hdfs://192.168.1.101/data/hailiang/news/201702/01/1485892039000-ng-news-0002;
				countF.print(localFileStatus.getPath().toString().substring(35) + ":");// news/201702/01/1485892039000-ng-news-0002:

				BytesWritable key = new BytesWritable();
				MapWritable value = new MapWritable();
				SequenceFile.Reader reader = new SequenceFile.Reader(conf,
						SequenceFile.Reader.file(localFileStatus.getPath()));

				// lzo文件解压并写入新文件中
				File localPath = new File("./".concat(localFileStatus.getPath().getParent().toString().substring(21)));
				if (!localPath.exists()) {
					localPath.mkdirs();
					System.out.println("Create folder: " + localPath.toString());
				}
				PrintWriter pw = new PrintWriter(
						new FileWriter("./".concat(localFileStatus.getPath().toString().substring(21)).concat(".txt")));
				while (reader.next(key, value)) {
					Set<Writable> vs = value.keySet();// 字段集合
					String con =null;
					for (Writable it : vs) {
//						if (it.toString().equals("content")) {
//							if (value.get(it).toString().contains("交通")) {
//								pw.println(it + ":" + value.get(it));
//								count++;
//								countMonth++;
//							}
//						}
						if (it.toString().equals("content")) {
							con = value.get(it).toString().replaceAll("\n", " ");
						}
						if (it.toString().equals("keywords")) {
							if (value.get(it).toString().contains("交通事故")) {
								pw.println("&*%*zz*%*&");
								pw.println("content:" + con);
								con = null;
								pw.println(it + ":" + value.get(it));
								count++;
								countMonth++;
							}
						}
					}
				}
				countF.println(count);
				pw.close();
				reader.close();
			}
			countF.println("sum of a month:" + countMonth);
			countF.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
