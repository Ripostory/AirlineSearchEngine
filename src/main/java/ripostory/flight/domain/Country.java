package ripostory.flight.domain;

public class Country {
	String name = "UNSET";
	int airports = -1;
	
	public Country(String name, int airports) {
		this.name = name;
		this.airports = airports;
	}
}
