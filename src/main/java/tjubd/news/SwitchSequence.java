package tjubd.news;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

public class SwitchSequence {
	public static void main(String[] args) throws IOException {
		//一个月数据一个线程
		MyThread mt1 = new MyThread("Jan",new Path("/data/hailiang/news/201701/"));
		MyThread mt2 = new MyThread("Feb",new Path("/data/hailiang/news/201702/"));
		MyThread mt3 = new MyThread("Mar",new Path("/data/hailiang/news/201703/"));
		MyThread mt4 = new MyThread("Apr",new Path("/data/hailiang/news/201704/"));
		MyThread mt5 = new MyThread("May",new Path("/data/hailiang/news/201705/"));
		MyThread mt6 = new MyThread("Jun",new Path("/data/hailiang/news/201706/"));
		
		mt1.start();
		mt2.start();
		mt3.start();
		mt4.start();
		mt5.start();
		mt6.start();
	}
}
