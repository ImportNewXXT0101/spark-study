package cn.spark.study.core.createrdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * 使用HDFS文件创建RDD
 * 案例：统计文本文件字数
 *
 */
public class HDFSFile {
	
	public static void main(String[] args) {
		// 创建JavaSparkContext
		// 修改：去除setMaster()设置，修改setAppName()
		SparkConf conf = new SparkConf()
				.setAppName("HDFSFile"); 
		JavaSparkContext sc = new JavaSparkContext(conf);

		// 使用SparkContext以及其子类的textFile()方法，针对HDFS文件创建RDD
		JavaRDD<String> lines = sc.textFile("hdfs://host101:9000/spark.txt");

		// 统计文本文件内的字符数
		JavaRDD<Integer> lineLength = lines.map(String::length);

		int count = lineLength.reduce((v1,v2)->v1+v2);

		System.out.println("文件总字符数是：" + count);
		// 关闭JavaSparkContext
		sc.close();
	}
	
}
