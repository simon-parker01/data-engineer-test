# Assumptions

*forcast side codes always integers and will remain below 2147483647
time in hours 0-24
wind direction is some kind of bearing i.e. if reading exceeds lowest value it start counting from lowest value again
wind speed meter reads in integers
wind gust meter/visibilty doesn't actually 2 decimal (even though it's always  xx.00 in the sample data)
screen temperature is in celsius and that the -99 degree readings are erroneous
pressure in millibars, censor doesn't read to 2 decimals
significant wether codes are Met office codes (https://www.metoffice.gov.uk/services/data/datapoint/code-definitions)
