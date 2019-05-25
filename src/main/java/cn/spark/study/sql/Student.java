package cn.spark.study.sql;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * 学生JavaBean
 *
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Student implements Serializable {

	private static final long serialVersionUID = -422071446486457663L;
	
	private int id;
	private String name;
	private int age;

}