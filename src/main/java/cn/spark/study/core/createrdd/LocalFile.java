package cn.spark.study.core.createrdd;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;

/**
 * 使用本地文件创建RDD
 * 案例：统计文本文件字数
 *
 */
public class LocalFile {
	
	public static void main(String[] args) {
		// 创建JavaSparkContext
		SparkConf conf = new SparkConf()
				.setAppName("LocalFile")
				.setMaster("local"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 使用SparkContext以及其子类的textFile()方法，针对本地文件创建RDD
		JavaRDD<String> lines = 
				sc.textFile("C://Users//Administrator//Desktop//spark.txt");

		JavaRDD<String[]> strings = lines.map(line -> line.split(" "));

		JavaRDD<Integer> lengthes = strings.map(s->s.length);
//		long count1 = strings.count();

		// 统计文本文件内的字符数
		JavaRDD<Integer> lineLength = lines.map(String::length);
		
		int count = lineLength.reduce((v1,v2)->v1+v2);
		int count1 = lengthes.reduce((v1,v2)->v1+v2);

		System.out.println("文件总字符数是：" + count);
		System.out.println("文件总单词数是：" + count1);

		// 关闭JavaSparkContext
		sc.close();
	}
	
}
