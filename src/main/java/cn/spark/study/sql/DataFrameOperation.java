package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * DataFrame的常用操作
 */
public class DataFrameOperation {

	public static void main(String[] args) {
		// 创建DataFrame
		SparkConf conf = new SparkConf()
				.setAppName("DataFrameCreate")
				.setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		// 创建出来的DataFrame完全可以理解为一张表
		DataFrame df = sqlContext.read().json("C://Users//Administrator//Desktop//students.json");
		
		// 打印DataFrame中所有的数据（select * from ...）
		df.show();
		// 打印DataFrame的元数据（Schema）
		df.printSchema();
		// 查询某列所有的数据
		df.select("name").show();  
		// 查询某几列所有的数据，并对列进行计算
		df.select(df.col("name"), df.col("score").plus(1)).show();
		// 根据某一列的值进行过滤
		df.filter(df.col("score").gt(80)).show();
		// 根据某一列进行分组，然后进行聚合
		df.groupBy(df.col("score")).count().show();
	}
	
}
