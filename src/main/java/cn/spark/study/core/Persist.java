package cn.spark.study.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;

/**
 * RDD持久化
 */
public class Persist {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("Persist")
				.setMaster("local"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// cache()或者persist()的使用
		// 必须在transformation或者textFile等创建了一个RDD之后，直接连续调用cache()或persist()才可以
		// 如果你先创建一个RDD，然后单独另起一行执行cache()或persist()方法，是没有用的
		// 而且，会报错，大量的文件会丢失
		JavaRDD<String> lines = sc.textFile("C://Users//Administrator//Desktop//spark.txt")
				.persist(StorageLevel.DISK_ONLY());

		long beginTime = System.currentTimeMillis();
		long count = lines.filter(line->line.length()>3).count();
		System.out.println("count1 : "+count);
		long endTime = System.currentTimeMillis();
		System.out.println("cost1 " + (endTime - beginTime) + " milliseconds.");
		/**
		 * count1 : 3
		 * cost1 406 milliseconds.
		 */



		beginTime = System.currentTimeMillis();
		count = lines.filter(line->line.length()>3).count();
		System.out.println("count2 : "+count);
		endTime = System.currentTimeMillis();
		System.out.println("cost2 " + (endTime - beginTime) + " milliseconds.");
		/**
		 * count2 : 3
		 * cost2 49 milliseconds.
		 */
		sc.close();
	}
	
}
