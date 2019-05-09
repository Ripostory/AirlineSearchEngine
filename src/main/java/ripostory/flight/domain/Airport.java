package ripostory.flight.domain;

public class Airport {
	
	String airportID;
	String name;
	String city;
	String country;
	String iata;
	String icao;
	String latitude;
	String longitude;
	String altitude;
	String timezone;
	String dst;
	String timeZone;
	String type;
	String source;
	
	public Airport(String airportID, String name, String city, String country, String iata, String icao, String latitude, 
			String longitude, String altitude, String timezone, String dst, String timeZone, String type, String source) {
		this.airportID = airportID;
		this.name = name;
		this.city = city;
		this.country = country;
		this.iata = iata;
		this.icao = icao;
		this.latitude = latitude;
		this.longitude = longitude;
		this.altitude = altitude;
		this.timezone = timezone;
		this.type = type;
		this.source = source;
	}
}
