package ripostory.flight;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * 
 * @author Ian Vanderhoff
 * 
 * 1. Find list of Airports operating in the Country X
 * 2. Find the list of Airlines having X stops
 * 3. List of Airlines operating with code share
 * 4. Find the list of Active Airlines in the United States
 */

public class Search {
	private JavaSparkContext context;

	public Search(JavaSparkContext con) {
		context = con;
	}
	
	/**
	 * Prints an RDD with all the airports in selected country.
	 * @param country
	 */
	public void airportsInCountry(final String country) {
		//retrieve airport data
		JavaRDD<String> rawAirData = new Data(context).retrieveData(DataType.airport);
		//format lines
		JavaRDD<String> rdd2 = rawAirData.flatMap(s -> Arrays.asList(s.split("\\r?\\n")).iterator());
		//remove non-applicable countries
		JavaRDD<String> rdd3 = rdd2.filter(line -> checkAirportCountry(line, country));
		
		for(String line:rdd3.collect()) {
			System.out.println(line);
		}
	}

	public void airlinesWithXStops(final int stops) {
		//retrieve route data
		JavaRDD<String> rawRouteData = new Data(context).retrieveData(DataType.route);
		//format RDD
		JavaRDD<String> routeRDD2 = rawRouteData.flatMap(s -> Arrays.asList(s.split("\\r?\\n")).iterator());
		//remove non-applicable routes
		JavaRDD<String> routeRDD3 = routeRDD2.filter(line -> checkStops(line, stops));
		
		for(String line:routeRDD3.collect()) {
			System.out.println(line);
		}
	}
	
	public void airlinesWithCodeShare() {
		//retrieve route data
		JavaRDD<String> rawRouteData = new Data(context).retrieveData(DataType.route);
		//format RDD
		JavaRDD<String> routeRDD2 = rawRouteData.flatMap(s -> Arrays.asList(s.split("\\r?\\n")).iterator());
		//remove non-applicable routes
		JavaRDD<String> routeRDD3 = routeRDD2.filter(line -> checkActiveRoute(line));
		
		HashSet<String> airlineIDsHashSet = new HashSet<String>();
		
		for(String line:routeRDD3.collect()) {
			//split on commas (csv)
			String[] entries = line.split(",");
			String code = entries[1];
			if (!airlineIDsHashSet.contains(code)) {
				airlineIDsHashSet.add(code);
			}
		}
				
		//retrieve airline data
		JavaRDD<String> rawAirlnData = new Data(context).retrieveData(DataType.airline);
		//format RDD
		JavaRDD<String> airlnRDD2 = rawAirlnData.flatMap(s -> Arrays.asList(s.split("\\r?\\n")).iterator());
		//remove non-applicable airlines
		JavaRDD<String> airlnRDD3 = airlnRDD2.filter(line -> checkAirlineIDs(line, airlineIDsHashSet));
		
		for(String line:airlnRDD3.collect()) {
			System.out.println(line);
		}
	}
	
	public void activeAirlinesInCountry(String country) {
		//retrieve airlines data
		JavaRDD<String> rawAirlnData = new Data(context).retrieveData(DataType.airline);
		//format RDD
		JavaRDD<String> airlnRDD2 = rawAirlnData.flatMap(s -> Arrays.asList(s.split("\\r?\\n")).iterator());
		//remove non-applicable airlines
		JavaRDD<String> airlnRDD3 = airlnRDD2.filter(line -> checkActiveAirlineCountry(line, country));
		
		for(String line:airlnRDD3.collect()) {
			System.out.println(line);
		}
	}
	
	/**
	 * Checks if the airport is in the searched for country.
	 * @param line
	 * @param country
	 * @return True if country match, false if not.
	 */
	public static Boolean checkAirportCountry(String line, final String country) {
		//split on commas (csv)
		String[] entries = line.split(",");
		String lineCountry = entries[3].replaceAll("\"", "");
		if (lineCountry.equals(country)) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public static Boolean checkStops(String line, final int stops) {
		//split on commas (csv)
		String[] entries = line.split(",");
		String lineStops = entries[7];
		if (lineStops.equals(Integer.toString(stops))) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public static Boolean checkActiveRoute(String line) {
		//split on commas (csv)
		String[] entries = line.split(",");
		String lineActive = entries[6];
		if (lineActive.equals("Y")) {
			return true;
		}
		else {
			return false;
		}
	}
	
	public static Boolean checkAirlineIDs(String line, HashSet<String> airlineIDsHashSet) {
		//split on commas (csv)
		String[] entries = line.split(",");
		String code = entries[0].replaceAll("\"", "");
		
		if (airlineIDsHashSet.contains(code)) {
			return true;
		}
		else {
			return false;
		}
	}
	
	private static Boolean checkActiveAirlineCountry(String line, String country) {
		//split on commas (csv)
		String[] entries = line.split(",");
		String lineCountry = entries[6].replaceAll("\"", "");
		String lineActive = entries[7].replaceAll("\"", "");
		if (lineCountry.equals(country)) {
			if (lineActive.equals("Y")) {
				return true;
			}
			else {
				return false;
			}
		}
		else {
			return false;
		}
	}
}
