package cn.spark.study.core.wordcount;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

/**
 * 排序的wordcount程序
 *
 */
public class SortWordCount {

	public static void main(String[] args) {
		// 创建SparkConf和JavaSparkContext
		SparkConf conf = new SparkConf()
				.setAppName("SortWordCount")
				.setMaster("local"); 
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 创建lines RDD
		JavaRDD<String> lines = sc.textFile("C://Users//Administrator//Desktop//spark.txt");
		
		JavaRDD<String> words = lines.flatMap(t->Arrays.asList(t.split(" ")));

		JavaPairRDD<String, Integer> pairs = words.mapToPair(t -> new Tuple2<>(t, 1));
		
		JavaPairRDD<String, Integer> wordCounts = pairs.reduceByKey(
				
				new Function2<Integer, Integer, Integer>() {

					private static final long serialVersionUID = 1L;

					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
					
				});
		
		//		新需求，是要按照每个单词出现次数的顺序，降序排序
		//   	需要将RDD转换成(3, hello) (2, you)的这种格式
		
		// 进行key-value的反转映射
		JavaPairRDD<Integer, String> countWords =
				wordCounts.mapToPair(t->new Tuple2<>(t._2, t._1));

		// 按照key进行排序
		JavaPairRDD<Integer, String> sortedCountWords = countWords.sortByKey(false);
		
		// 再次将value-key进行反转映射
		JavaPairRDD<String, Integer> sortedWordCounts =
				sortedCountWords.mapToPair(t->new Tuple2<>(t._2, t._1));
				
		sortedWordCounts.foreach(new VoidFunction<Tuple2<String,Integer>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1 + " appears " + t._2 + " times.");  	
			}
			
		});
		
		sc.close();
	}
	
}
