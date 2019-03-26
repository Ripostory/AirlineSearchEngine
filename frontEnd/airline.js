var xhttp = new XMLHttpRequest();

// Run on document loaded
$( document ).ready(function() {
    console.log( "ready!" );
    getTopCountry();
});

function getTopCountry() {
  xhttp.onreadystatechange = function() {
    if (this.readyState == 4 && this.status == 200) {
      var response = JSON.parse(this.responseText);
      $("#gate-country").html(response.country);
    }
  };
  xhttp.open("GET", "https://api.ripostory.com/air/gate/top-country", true);
  xhttp.send();
}

function getTopCities(cityCount, isIncoming) {
  //TODO implement
}
