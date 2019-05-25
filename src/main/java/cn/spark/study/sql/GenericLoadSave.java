package cn.spark.study.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * 通用的load和save操作
 *
 */
public class GenericLoadSave {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf() 
				.setAppName("GenericLoadSave").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
	
		DataFrame usersDF = sqlContext.read().json(
				"C://Users//Administrator//Desktop//students.json");
		usersDF.select("name", "score").write()
				.save("C://Users//Administrator//Desktop//namesAndFavColors.json");
	}
	
}
