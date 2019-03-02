package ripostory.flight;
import java.util.Arrays;

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
		 
		//Load data
		JavaRDD<String> textFile = new Data(context).retrieveData(DataType.route);
		
		//Perform map reduce
		JavaPairRDD<String, Integer> counts = textFile
                .flatMap(s -> Arrays.asList(s.split("[ ,]")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
		
		counts.foreach(p -> System.out.println(p));
		
		
		context.close();
 
	}
}