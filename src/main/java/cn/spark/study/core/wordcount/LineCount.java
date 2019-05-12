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
		// 创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("LineCount")
				.setMaster("local"); 
		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
	
		// 创建初始RDD，lines，每个元素是一行文本
		JavaRDD<String> lines = sc.textFile("C://Users//Administrator//Desktop//spark.txt");
		
		// 对lines RDD执行mapToPair算子，将每一行映射为(line, 1)的这种key-value对的格式
		// 然后后面才能统计每一行出现的次数
		JavaPairRDD<String, Integer> pairs = lines.mapToPair(line->new Tuple2<>(line, 1));

		// 对pairs RDD执行reduceByKey算子，统计出每一行出现的总次数
		JavaPairRDD<String, Integer> lineCounts = pairs.reduceByKey((v1 , v2)->v1 + v2);
				

		// 执行一个action操作，foreach，打印出每一行出现的次数
		lineCounts.foreach(
				t->System.out.println(t._1 + " appears " + t._2 + " times."));

		// 关闭JavaSparkContext
		sc.close();
	}
	
}
