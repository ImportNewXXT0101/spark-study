package cn.spark.study.core.operation;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;

import org.junit.Test;
import scala.Tuple2;

import static java.util.stream.Collectors.toList;

/**
 * 	transformation操作
 *
 * 		 1 map
 * 		 2 filter
 * 		 3 flatMap
 * 		 4 groupByKey
 * 		 5 reduceByKey
 * 		 6 sortByKey
 * 		 7 join
 * 		 8 cogroup
 */
@SuppressWarnings(value = {"all"})
public class TransformationOperation {

	private static void prettyPrint(Object object){
		System.out.println("------------------");
		System.out.println("--->>  "+object);
	}

	public static void main(String[] args) {
		 map();
		 filter();
		 flatMap();
		 groupByKey();
		 reduceByKey();
		 sortByKey();
		 join();
		 cogroup();
	}

	/**
	 * map 将集合中每一个元素都乘以2
	 */
	private static void map() {
		// 创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("map")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
	
		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
		
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		
		JavaRDD<Integer> multipleNumberRDD = numberRDD.map(v -> v * 2);

		multipleNumberRDD.foreach(number->prettyPrint(number));
		
		sc.close();
	}
	
	/**
	 * filter 过滤集合中的偶数
	 */
	private static void filter() {
		// 创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("filter")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
		
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		
		JavaRDD<Integer> evenNumberRDD = numberRDD.filter(v -> v % 2 == 0);

		evenNumberRDD.foreach(number->prettyPrint(number));
		
		sc.close();
	}
	
	/**
	 * flatMap 将文本行拆分为多个单词
	 */
	private static void flatMap() {
		// 创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("flatMap")  
				.setMaster("local");  
		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<String> lineList = Arrays.asList("hello you", "hello me", "hello world");
		
		JavaRDD<String> lines = sc.parallelize(lineList);
		
		JavaRDD<String> words = lines.flatMap(t->Arrays.asList(t.split(" ")));
		// 打印新的RDD
		words.foreach(new VoidFunction<String>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(String t) throws Exception {
				System.out.println(t);
			}
		});

		// 关闭JavaSparkContext
		sc.close();
	}
	
	/**
	 * groupByKey 按照班级对成绩进行分组
	 */
	private static void groupByKey() {
		// 创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("groupByKey")  
				.setMaster("local");
		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 模拟集合
		List<Tuple2<String, Integer>> scoreList = Arrays.asList(
				new Tuple2<String, Integer>("class1", 80),
				new Tuple2<String, Integer>("class2", 75),
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class2", 65));
		
		// 并行化集合，创建JavaPairRDD
		JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);
		
		JavaPairRDD<String, Iterable<Integer>> groupedScores = scores.groupByKey();
		
		// 打印groupedScores RDD
		groupedScores.foreach(new VoidFunction<Tuple2<String,Iterable<Integer>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Iterable<Integer>> t)
					throws Exception {
				System.out.println("class: " + t._1);  
				Iterator<Integer> ite = t._2.iterator();
				while(ite.hasNext()) {
					System.out.println(ite.next());  
				}
				System.out.println("==============================");   
			}
			
		});
		
		// 关闭JavaSparkContext
		sc.close();
	}
	
	/**
	 * reduceByKey 统计每个班级的总分
	 */
	private static void reduceByKey() {
		// 创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("reduceByKey")  
				.setMaster("local");
		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 模拟集合
		List<Tuple2<String, Integer>> scoreList = Arrays.asList(
				new Tuple2<String, Integer>("class1", 80),
				new Tuple2<String, Integer>("class2", 75),
				new Tuple2<String, Integer>("class1", 90),
				new Tuple2<String, Integer>("class2", 65));
		
		// 并行化集合，创建JavaPairRDD
		JavaPairRDD<String, Integer> scores = sc.parallelizePairs(scoreList);
		
		JavaPairRDD<String, Integer> totalScores = scores.reduceByKey(
				
				new Function2<Integer, Integer, Integer>() {
					
					private static final long serialVersionUID = 1L;
					
					// 对每个key，都会将其value，依次传入call方法
					// 从而聚合出每个key对应的一个value
					// 然后，将每个key对应的一个value，组合成一个Tuple2，作为新RDD的元素
					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
					
				});
		
		// 打印totalScores RDD
		totalScores.foreach(new VoidFunction<Tuple2<String,Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> t) throws Exception {
				System.out.println(t._1 + ": " + t._2);   
			}
			
		});
		
		// 关闭JavaSparkContext
		sc.close();
	}
	
	/**
	 * sortByKey案例：按照学生分数进行排序
	 */
	private static void sortByKey() {
		// 创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("sortByKey")  
				.setMaster("local");
		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 模拟集合
		List<Tuple2<Integer, String>> scoreList = Arrays.asList(
				new Tuple2<Integer, String>(65, "leo"),
				new Tuple2<Integer, String>(50, "tom"),
				new Tuple2<Integer, String>(100, "marry"),
				new Tuple2<Integer, String>(80, "jack"));
		
		// 并行化集合，创建RDD
		JavaPairRDD<Integer, String> scores = sc.parallelizePairs(scoreList);
		
		JavaPairRDD<Integer, String> sortedScores = scores.sortByKey(false);
		
		// 打印sortedScored RDD
		sortedScores.foreach(new VoidFunction<Tuple2<Integer,String>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<Integer, String> t) throws Exception {
				System.out.println(t._1 + ": " + t._2);  
			}
			
		});
		
		// 关闭JavaSparkContext
		sc.close();
	}
	
	/**
	 * join案例：打印学生成绩
	 */
	private static void join() {
		// 创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("join")  
				.setMaster("local");
		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 模拟集合
		List<Tuple2<Integer, String>> studentList = Arrays.asList(
				new Tuple2<Integer, String>(1, "leo"),
				new Tuple2<Integer, String>(2, "jack"),
				new Tuple2<Integer, String>(3, "tom"));
		
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer, Integer>(1, 100),
				new Tuple2<Integer, Integer>(2, 90),
				new Tuple2<Integer, Integer>(3, 60));
		// 并行化两个RDD
		JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
		JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);
		
		// 使用join算子关联两个RDD
		// join以后，还是会根据key进行join，并返回JavaPairRDD
		// 但是JavaPairRDD的第一个泛型类型，之前两个JavaPairRDD的key的类型，因为是通过key进行join的
		// 第二个泛型类型，是Tuple2<v1, v2>的类型，Tuple2的两个泛型分别为原始RDD的value的类型
		// join，就返回的RDD的每一个元素，就是通过key join上的一个pair
		// 什么意思呢？比如有(1, 1) (1, 2) (1, 3)的一个RDD
			// 还有一个(1, 4) (2, 1) (2, 2)的一个RDD
			// 如果是cogroup的话，会是(1,((1,2,3),(4)))    
			// join以后，实际上会得到(1 (1, 4)) (1, (2, 4)) (1, (3, 4))	
		JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores1 = students.join(scores);
		JavaPairRDD<Integer, Tuple2<String, Integer>> studentScores2 = students.join(scores);

		JavaPairRDD<Integer, Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> join
				= studentScores1.join(studentScores2);

		join.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>>>() {
			@Override
			public void call(Tuple2<Integer, Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> t) throws Exception {
				System.out.println("+++");
				System.out.println(t._1 +" "+ t._2.toString());
				System.out.println("+++");
			}
		});

		// 打印studnetScores RDD
		studentScores1.foreach(
				
				new VoidFunction<Tuple2<Integer,Tuple2<String,Integer>>>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public void call(Tuple2<Integer, Tuple2<String, Integer>> t)
							throws Exception {
						System.out.println("student id: " + t._1);  
						System.out.println("student name: " + t._2._1);  
						System.out.println("student score: " + t._2._2);
						System.out.println("===============================");   
					}
					
				});
		
		// 关闭JavaSparkContext
		sc.close();
	}
	
	/**
	 * cogroup 打印学生成绩
	 */
	private static void cogroup() {
		// 创建SparkConf
		SparkConf conf = new SparkConf()
				.setAppName("cogroup")  
				.setMaster("local");
		// 创建JavaSparkContext
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 模拟集合
		List<Tuple2<Integer, String>> studentList = Arrays.asList(
				new Tuple2<Integer, String>(1, "leo"),
				new Tuple2<Integer, String>(2, "jack"),
				new Tuple2<Integer, String>(3, "tom"));
		
		List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
				new Tuple2<Integer, Integer>(1, 100),
				new Tuple2<Integer, Integer>(2, 90),
				new Tuple2<Integer, Integer>(3, 60),
				new Tuple2<Integer, Integer>(1, 70),
				new Tuple2<Integer, Integer>(2, 80),
				new Tuple2<Integer, Integer>(3, 50));
		
		// 并行化两个RDD
		JavaPairRDD<Integer, String> students = sc.parallelizePairs(studentList);
		JavaPairRDD<Integer, Integer> scores = sc.parallelizePairs(scoreList);
		
		// cogroup与join不同
		// 相当于是，一个key join上的所有value，都给放到一个Iterable里面去了 
		JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> studentScores =
				students.cogroup(scores);
		
		// 打印studnetScores RDD
		studentScores.foreach(
				
				new VoidFunction<Tuple2<Integer,Tuple2<Iterable<String>,Iterable<Integer>>>>() {

					private static final long serialVersionUID = 1L;
		
					@Override
					public void call(
							Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t)
							throws Exception {
						System.out.println("student id: " + t._1);  
						System.out.println("student name: " + t._2._1);  
						System.out.println("student score: " + t._2._2);
						System.out.println("===============================");   
					}
					
				});
		
		// 关闭JavaSparkContext
		sc.close();
	}
	
}
