# Assumptions

* forcast side codes always integers and will remain below 2147483647
* time in hours 0-24
* wind direction is some kind of bearing i.e. if reading exceeds lowest value it start counting from lowest value again
* wind speed meter reads in integers
* gust/visibilty/pressure sensors don't acutally read to 2 decimal places (even though it's provided as a float, it's always  xx.00 in the sample data)
* screen temperature is in degrees celsius and that the -99 degree readings are erroneous values
* pressure in millibars
* significant wether codes are Met office codes (https://www.metoffice.gov.uk/services/data/datapoint/code-definitions)
