package ripostory.flight;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.Gson;

import ripostory.flight.domain.Country;
import scala.Tuple2;

/**
 * 
 * @author Ronn
 * This serves to implement batch jobs that provide info on
 * top country airport count and top airport airline count
 *
 */

public class Aggregation {
	private JavaSparkContext context;
	
	public Aggregation(JavaSparkContext con) {
		context = con;
	}
	
	/**
	 * Gets the largest number of airports by country. Implements the second aggragation
	 * job
	 * @return Country and airport count
	 */
	public String highestAirportCount() {
		//retrieve airport data
		JavaRDD<String> rawAirData = new Data(context).retrieveData(DataType.airport);
		
		//Perform map reduce
		Tuple2<String, Integer> counts = rawAirData
				
				//Split lines
                .flatMap(s -> Arrays.asList(s.split("\\r?\\n")).iterator())
                
                //Map each country to a value of 1
                .mapToPair(line -> new Tuple2<>(parseCountry(line), 1))
                
                //Add each tuple to get final count
                .reduceByKey((a, b) -> a + b)
                
                //Find max
                .reduce((c1, c2) -> {if (c1._2 < c2._2) return c2; else return c1;});
		
		//fill objects and serialize
		Country topCountry = new Country(counts._1, counts._2);
		Gson gson = new Gson();
		
		return gson.toJson(topCountry);
	}
	
	/**
	 * 
	 * @param cityCount how many cities to display
	 * @param isIncoming sort by either incoming or outgoing TODO finish
	 * @return
	 */
	public String flightsByCity(int cityCount, boolean isIncoming) {
		return "";
	}
	
	/**
	 * Parses a country from the format provided by airports.dat
	 * @param line
	 * @return name of the country
	 */
	public static String parseCountry(String line) {
		//split on commas (csv)
		String[] entries = line.split(",");
		return entries[3].replaceAll("\"", "");
	}
}
