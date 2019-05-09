package ripostory.flight.domain;

public class Airline {
	
	String airlineID;
	String name;
	String alias;
	String iata;
	String icao;
	String callsign;
	String country;
	String active;
	
	public Airline(String airlineID, String name, String alias, String iata, String icao, 
			String callsign, String country, String active) {
		this.airlineID = airlineID;
		this.name = name;
		this.alias = alias;
		this.iata = iata;
		this.icao = icao;
		this.callsign = callsign;
		this.country = country;
		this.active = active;
	}

}
