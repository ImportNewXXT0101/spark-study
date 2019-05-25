package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 手动指定数据源类型
 *
 */
public class ManuallySpecifyOptions {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()   
				.setAppName("ManuallySpecifyOptions").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
		
		DataFrame peopleDF = sqlContext.read().format("json")
				.load("C://Users//Administrator//Desktop//students.json");
		peopleDF.select("name").write().format("json")
				.save("C://Users//Administrator//Desktop//people.json");
	}
	
}
