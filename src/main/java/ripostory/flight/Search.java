package ripostory.flight;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.gson.Gson;

import ripostory.flight.domain.Airline;
import ripostory.flight.domain.Airport;

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
	 * Returns a list of airports in the country specified by the parameter. Airports are in Json format.
	 * @param String country
	 * @return ArrayList<String>
	 */
	public ArrayList<String> airportsInCountry(final String country) {
		//retrieve airport data
		JavaRDD<String> rawAirData = new Data(context).retrieveData(DataType.airport);
		//format lines
		JavaRDD<String> rdd2 = rawAirData.flatMap(s -> Arrays.asList(s.split("\\r?\\n")).iterator());
		//remove non-applicable countries
		JavaRDD<String> rdd3 = rdd2.filter(line -> checkAirportCountry(line, country));
		
		// collect the RDD from cluster machines into list in driver program.
		List<String> collectedRddList = rdd3.collect();
		String[] collectedRddArr = collectedRddList.toArray(new String[0]);
		
		// format the fields, add to ArrayList
		ArrayList<String[]> formatedStrings = new ArrayList<String[]>();
		for(String s : collectedRddList) {
			String[] sArr = splitStringOnCommas(s);
			formatedStrings.add(removeQuotesFromStrings(sArr));
		}
		
		// Add fields to Airport object, then convert object to Json string and store in ArrayList
		ArrayList<String> jsonArrLst = new ArrayList<String>();
		for(String[] sArr : formatedStrings) {
			Airport airportObj = new Airport(sArr[0], sArr[1], sArr[2], sArr[3], sArr[4], sArr[5], sArr[6], sArr[7], sArr[8], sArr[9], sArr[10], sArr[11], sArr[12], sArr[13]);			
			Gson gson = new Gson();
			jsonArrLst.add(gson.toJson(airportObj));
		}
		// return list
		return jsonArrLst;
	}

	/**
	 * Returns a list of airlines whose routes have number of stops specified by parameter. List in Json format.
	 * @param stops
	 * @return ArrayList<String>
	 */
	public ArrayList<String> airlinesWithXStops(final int stops) {
		//retrieve route data
		JavaRDD<String> rawRouteData = new Data(context).retrieveData(DataType.route);
		//format RDD
		JavaRDD<String> routeRDD2 = rawRouteData.flatMap(s -> Arrays.asList(s.split("\\r?\\n")).iterator());
		//remove non-applicable routes
		JavaRDD<String> routeRDD3 = routeRDD2.filter(line -> checkStops(line, stops));
		
		// Hash set for IDs of airlines who have x stops.
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
		
		// collect the RDD from cluster machines into list in driver program.
		List<String> collectedRddList = airlnRDD3.collect();
		String[] collectedRddArr = collectedRddList.toArray(new String[0]);
		
		// format the fields, add to ArrayList
		ArrayList<String[]> formatedStrings = new ArrayList<String[]>();
		for(String s : collectedRddList) {
			String[] sArr = splitStringOnCommas(s);
			formatedStrings.add(removeQuotesFromStrings(sArr));
		}
		
		// Add fields to Airline object, then convert object to Json string and store in ArrayList
		ArrayList<String> jsonArrLst = new ArrayList<String>();
		for(String[] sArr : formatedStrings) {
			Airline airlineObj = new Airline(sArr[0], sArr[1], sArr[2], sArr[3], sArr[4], sArr[5], sArr[6], sArr[7]);			
			Gson gson = new Gson();
			jsonArrLst.add(gson.toJson(airlineObj));
		}
		// return list
		return jsonArrLst;
	}
	
	/**
	 * Returns a list of airlines that operate with codeshare. List in Json format.
	 * @return ArrayList<String>
	 */
	public ArrayList<String> airlinesWithCodeShare() {
		//retrieve route data
		JavaRDD<String> rawRouteData = new Data(context).retrieveData(DataType.route);
		//format RDD
		JavaRDD<String> routeRDD2 = rawRouteData.flatMap(s -> Arrays.asList(s.split("\\r?\\n")).iterator());
		//remove non-applicable routes
		JavaRDD<String> routeRDD3 = routeRDD2.filter(line -> checkActiveRoute(line));
		
		// Hash set for IDs of airlines who have code share
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
		
		// collect the RDD from cluster machines into list in driver program.
		List<String> collectedRddList = airlnRDD3.collect();
		String[] collectedRddArr = collectedRddList.toArray(new String[0]);
		
		// format the fields, add to ArrayList
		ArrayList<String[]> formatedStrings = new ArrayList<String[]>();
		for(String s : collectedRddList) {
			String[] sArr = splitStringOnCommas(s);
			formatedStrings.add(removeQuotesFromStrings(sArr));
		}
		
		// Add fields to Airline object, then convert object to Json string and store in ArrayList
		ArrayList<String> jsonArrLst = new ArrayList<String>();
		for(String[] sArr : formatedStrings) {
			Airline airlineObj = new Airline(sArr[0], sArr[1], sArr[2], sArr[3], sArr[4], sArr[5], sArr[6], sArr[7]);			
			Gson gson = new Gson();
			jsonArrLst.add(gson.toJson(airlineObj));
		}
		// return list
		return jsonArrLst;
	}
	
	/**
	 * Returns a list of airlines operating in country specified by parameter. List in Json format.
	 * @param country
	 * @return ArrayList<String>
	 */
	public ArrayList<String> activeAirlinesInCountry(String country) {
		//retrieve airlines data
		JavaRDD<String> rawAirlnData = new Data(context).retrieveData(DataType.airline);
		//format RDD
		JavaRDD<String> airlnRDD2 = rawAirlnData.flatMap(s -> Arrays.asList(s.split("\\r?\\n")).iterator());
		//remove non-applicable airlines
		JavaRDD<String> airlnRDD3 = airlnRDD2.filter(line -> checkActiveAirlineCountry(line, country));
		
		// collect the RDD from cluster machines into list in driver program.
		List<String> collectedRddList = airlnRDD3.collect();
		String[] collectedRddArr = collectedRddList.toArray(new String[0]);
		
		// format the fields, add to ArrayList
		ArrayList<String[]> formatedStrings = new ArrayList<String[]>();
		for(String s : collectedRddList) {
			String[] sArr = splitStringOnCommas(s);
			formatedStrings.add(removeQuotesFromStrings(sArr));
		}
		
		// Add fields to Airline object, then convert object to Json string and store in ArrayList
		ArrayList<String> jsonArrLst = new ArrayList<String>();
		for(String[] sArr : formatedStrings) {
			Airline airlineObj = new Airline(sArr[0], sArr[1], sArr[2], sArr[3], sArr[4], sArr[5], sArr[6], sArr[7]);			
			Gson gson = new Gson();
			jsonArrLst.add(gson.toJson(airlineObj));
		}
		// return list
		return jsonArrLst;
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
	
	/**
	 * Checks if the airline has the number of stops specified by parameter.
	 * @param line
	 * @param stops
	 * @return True if stops match, false if not.
	 */
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
	
	/**
	 * Checks if the route is active.
	 * @param line
	 * @return True if active, false if not.
	 */
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
	
	/**
	 * Checks if the airline ID is contained in the hash set.
	 * @param line
	 * @param airlineIDsHashSet
	 * @return True if contains code, false if not.
	 */
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
	
	/**
	 * Checks if the airline operates in country specified by parameter. If so, checks if airline is active.
	 * @param line
	 * @param country
	 * @return True if active and in country, false if not.
	 */
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
	
	/**
	 * Splits a comma separated string into an array of Strings.
	 * @param String
	 * @return String[]
	 */
	private static String[] splitStringOnCommas(String line) {
		//split on commas (csv)
		String[] entries = line.split(",");
		return entries;
	}
	
	/**
	 * Removes quotation marks from around the words in the String array.
	 * @param String[]
	 * @return String[]
	 */
	private static String[] removeQuotesFromStrings(String[] stringArr) {
		ArrayList<String> tempArrLst = new ArrayList<String>();
		for(String word : stringArr) {
			tempArrLst.add(word.replaceAll("\"", ""));
//			if(word.replaceAll("\"", "").isEmpty()) {
//				tempArrLst.add("NULL");
//			}
//			else {
//				tempArrLst.add(word.replaceAll("\"", ""));
//			}
		}
		String[] tempArr = tempArrLst.toArray(new String[0]);
		return tempArr;
	}
}
