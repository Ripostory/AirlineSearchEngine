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

function getTopCities() {
  //definition on retrieve data
  xhttp.onreadystatechange = function() {
    if (this.readyState == 4 && this.status == 200) {
      var response = JSON.parse(this.responseText);

      //add all entries into table
      var rank = 1;
      var finalHTML = "";
      response.cities.forEach( function (entry) {
        finalHTML += addCityEntry(rank, entry);
        rank++;
      });
      $("#table-topcity").html(finalHTML);
    }
  };

  var form = $("#form-topcity");
  if (form.find('input[name="count"]').val() > 0 && form.find('input[name="count"]').val() != "") {
    //clear data
    $("#table-topcity").html("");

    //retrieve data from server
    xhttp.open("GET", "https://api.ripostory.com/air/gate/top-city?" + form.serialize(), true);
    xhttp.send();
  }

  return false;
}

function addCityEntry(rank, entry) {
  return '<tr><th scope="row">' + rank + '</th><td>' + entry.name +
    '</td><td>' + entry.incoming + '</td><td>' + entry.outgoing +'</td></tr>';
}
