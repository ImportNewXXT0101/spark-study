package cn.spark.study.core.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 统计每行出现的次数
 */
@SuppressWarnings("all")
public class LineCount {

	public static void main(String[] args) {
		// 创建JavaSparkContext
		SparkConf conf = new SparkConf()
				.setAppName("LineCount")
				.setMaster("local"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
	
		JavaRDD<String> lines = sc.textFile("C://Users//Administrator//Desktop//spark.txt");

		JavaPairRDD<String, Integer> pairs = lines.mapToPair(line->new Tuple2<>(line, 1));

		JavaPairRDD<String, Integer> lineCounts = pairs.reduceByKey((v1 , v2)->v1 + v2);

		lineCounts.foreach(
				t->System.out.println(t._1 + " appears " + t._2 + " times."));

		sc.close();
	}
	
}
