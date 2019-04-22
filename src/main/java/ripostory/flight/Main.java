package ripostory.flight;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class Main {
	public static void main(String[] args) {
		 
		SparkConf sparkConf = new SparkConf();
 
		sparkConf.setAppName("Hello Spark");
		//TODO this will be changed once deployed to a cluster
		sparkConf.setMaster("local[2]");
 
		JavaSparkContext context = new JavaSparkContext(sparkConf);
		
		Search test = new Search(context);
		test.airportsInCountry("France");
		test.airlinesWithXStops(1);
		test.airlinesWithCodeShare();
		test.activeAirlinesInCountry("France");
		
		String airports = new Aggregation(context).highestAirportCount();
		System.out.println(airports);
		
		context.close();
 
	}
}