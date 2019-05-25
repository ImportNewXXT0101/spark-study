package cn.spark.study.sql;

import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * 使用反射的方式将RDD转换为DataFrame
 *
 */
public class RDD2DataFrameReflection {

	public static void main(String[] args) {
		// 创建普通的RDD
		SparkConf conf = new SparkConf()
				.setMaster("local")  
				.setAppName("RDD2DataFrameReflection");  
		JavaSparkContext sc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(sc);
	
		JavaRDD<String> lines = sc.textFile("C://Users//Administrator//Desktop//students.txt");
		
		JavaRDD<Student> students = lines.map(new Function<String, Student>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Student call(String line) throws Exception {
				String[] lineSplited = line.split(",");  
				Student stu = new Student();
				stu.setId(Integer.valueOf(lineSplited[0].trim()));  
				stu.setName(lineSplited[1]);  
				stu.setAge(Integer.valueOf(lineSplited[2].trim())); 
				return stu;
			}
			
		});
		
		// 使用反射方式，将RDD转换为DataFrame
		// 将Student.class传入进去，其实就是用反射的方式来创建DataFrame
		// 因为Student.class本身就是反射的一个应用
		// 然后底层还得通过对Student Class进行反射，来获取其中的field
		// 这里要求，JavaBean必须实现Serializable接口，是可序列化的
		DataFrame studentDF = sqlContext.createDataFrame(students, Student.class);
		
		// 拿到了一个DataFrame之后，就可以将其注册为一个临时表，然后针对其中的数据执行SQL语句
		studentDF.registerTempTable("students");  
		
		// 针对students临时表执行SQL语句，查询年龄小于等于18岁的学生，就是teenageer
		DataFrame teenagerDF = sqlContext.sql("select * from students where age<= 18");  

		teenagerDF.show();

		// 将查询出来的DataFrame，再次转换为RDD
		JavaRDD<Row> teenagerRDD = teenagerDF.javaRDD();

        // 将RDD中的数据，进行映射，映射为Student
        // 将数据collect回来，打印出来
        System.out.println("-------------------");
        JavaRDD<Student> studentJavaRDD = teenagerRDD.map(row ->
                Student.builder()
                        .id(row.getInt(1))
                        .name(row.getString(2))
                        .age(row.getInt(0))
                        .build());
        studentJavaRDD.collect().forEach(System.out::println);
        System.out.println("-------------------");

	}
	
}
