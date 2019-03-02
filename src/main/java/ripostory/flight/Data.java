package ripostory.flight;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * @author Ronn
 * A broker for retrieving Data for Spark. abstracted so that 
 * it can use either local files or HDFS depending on if the build
 * is either development or production
 *
 */

enum DataType {
	airline,
	airport,
	plane,
	route
}

public class Data {
	private boolean prod = false;
	private String hdfs = "hdfs:///tmp/";
	private String local = "src/main/resources/";
	private JavaSparkContext context = null;
	
	public Data(JavaSparkContext con) {
		context = con;
	}
	
	public Data(JavaSparkContext con, boolean isProd) {
		context = con;
		prod = isProd;
	}
	
	public JavaRDD<String> retrieveData(DataType type) {
		String pre = local;
		if (prod) {
			pre = hdfs;
		}
		
		String full;
		switch (type) {
		case airline:
			full = pre + "airlines.dat";
			break;
		case airport:
			full = pre + "airports.dat";
			break;
		case plane:
			full = pre + "planes.dat";
			break;
		case route:
			full = pre + "routes.dat";
			break;
		default:
			full = pre + "airlines.dat";
		}
		return context.textFile(full);
	}
}
