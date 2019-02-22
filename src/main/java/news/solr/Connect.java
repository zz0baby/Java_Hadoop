package news.solr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

public class Connect {
	public static void main(String[] args) throws SolrServerException, IOException {
		String url = "http://172.28.9.62:8983/solr/news";
		System.out.println("开始连接solr");
		SolrClient solrClient = new HttpSolrClient.Builder(url).build();
		System.out.println("solr连接成功");
		SolrInputDocument document = new SolrInputDocument();

		// 字段集合
		BufferedReader keyR = new BufferedReader(new FileReader("/mnt/lun1/data/tjubd/WS/W/keysets.csv"));
		ArrayList<String> keysets = new ArrayList<String>();
		String line;
		while ((line = keyR.readLine()) != null) {
			keysets.add(line);
		}
		keyR.close();

		// 读数据
		File newsF = new File("/mnt/lun1/data/tjubd/zz/zztt/news/201701");
		File[] monthF = newsF.listFiles();
		for (File fm : monthF) {
			File[] daysF = newsF.listFiles();
			for (File f : daysF) {// 读一天
				String[] file = f.list();
				for (int i = 0; i < file.length; i++) {// 读一天中的每一个文件
					BufferedReader bw = new BufferedReader(new FileReader(new File(f, file[i])));
					String content = null;// 记录字段内容
					String kName = null;// 记录字段名
					while ((line = bw.readLine()) != null) {
						String[] tokens = line.split(":");
						if (line.equals("&*%*zz*%*&")) {// 新的新闻
							solrClient.add(document);
							continue;
						}
						if (!keysets.contains(tokens[0])) {// 非字段名
							content = content.concat(line);
						} else {
							if (kName != null) {
								document.addField(kName, content);
							}
							kName = tokens[0];// 取字段
							content = line.substring(line.indexOf(":") + 1);// 取内容
						}
					}
				}
			}
		}
		solrClient.commit();
	}
}