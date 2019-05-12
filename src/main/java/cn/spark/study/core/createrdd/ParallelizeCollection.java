package cn.spark.study.core.createrdd;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

/**
 * 并行化集合创建RDD
 * 案例：累加1到10
 *
 */
public class ParallelizeCollection {
	
	public static void main(String[] args) {
		// 创建JavaSparkContext
		SparkConf conf = new SparkConf()
				.setAppName("ParallelizeCollection")
				.setMaster("local");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		// 通过并行化集合的方式创建RDD，
		// 调用SparkContext以及其子类的parallelize()方法
		List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5, 6);
		JavaRDD<Integer> numberRDD = sc.parallelize(numbers);
		
		// 执行reduce算子操作
		int sum = numberRDD.reduce((v1,v2)->v1+v2);
		
		// 输出累加的和
		System.out.println(numbers+" 累加和：-> " + sum);
		
		// 关闭JavaSparkContext
		sc.close();
	}
	
}
