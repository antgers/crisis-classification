library('data.table')
library('RCurl')
library('rjson')
library('jsonlite')
library('lubridate')

# read the urls from json file
json_urls <- fromJSON("urls.json")

json.size <- length(json_urls$value$name)

for ( i in 1:json.size ){
  
  read.json.data <- fromJSON( json_urls$value$url[i]) 
  df <- as.data.frame(read.json.data)
  if ( length(names(df)) != length(colnames(read.json.data$value)) ){
    names(df) <- c( names(df)[i], colnames(read.json.data$value) )
  }else{
    names(df) <- colnames(read.json.data$value)
  }
    
  assign(json_urls$value$name[i], df)
  
}

#---------------------------------------- Retrieve data through Queries

service_root_URI <- "https://beaware.server.de/SensorThingsService/v1.0/"

SensorThing.Entities <- c('Things', 'Location', 'HistoricalLocations', 'Datastreams', 'Sensor', 'Observation', 'ObservedProperties', 
                          'FeaturesOfInterest', 'MultiDatastreams')

#----------- Extract River data

# river <- fromJSON("https://beaware.server.de/SensorThingsService/v1.0/Things?$filter=properties/type%20eq%20%27river%27")

# create Resource Path
resource_path <- paste0( service_root_URI, SensorThing.Entities[1] )

Things.obj <- paste0('%27','river', '%27')
Things.property <- paste0('properties/type')

query <- paste0( resource_path, '?$filter', '=', Things.property, '%20eq%20',  Things.obj )
river <- fromJSON( query )

#----------- Extract Station data

# stations <- fromJSON("https://beaware.server.de/SensorThingsService/v1.0/Things?$filter=properties/type%20eq%20%27station%27")

Things.obj <- paste0('%27','station', '%27')
query <- paste0( resource_path, '?$filter', '=', Things.property, '%20eq%20',  Things.obj )
stations <- fromJSON( query ) 

# Select the "Bacchiglione a Longare CAE" station
# stationBL <- fromJSON("https://beaware.server.de/SensorThingsService/v1.0/Things?$filter=name%20eq%20%27Bacchiglione%20a%20Longare%20CAE%27%20and%20properties/type%20eq%20%27station%27")

station.name <- paste0('%27','Bacchiglione','%20','a','%20','Longare','%20','CAE','%27')
operator <- paste0('%20', 'and', '%20') 
comparison.operator <- paste0('%20','eq','%20' )
command <- '$filter' 
Things.property.1 <- paste0('properties/type')
Things.obj.1 <- paste0('%27','station', '%27')
Things.property.2 <- paste0('name')
Things.obj.2 <- station.name

query <- paste0( resource_path, "?", command, "=", Things.property.1, comparison.operator, Things.obj.1, operator, Things.property.2, comparison.operator, Things.obj.2) 

stationBL <- fromJSON( query )
  




#--------------------------------
# region Grantorto

station.name <- paste0('%27','Grantorto','%27')

# Extract data from one day from the 3 datastreams

airTemp <- fromJSON("https://beaware.server.de/SensorThingsService/v1.0/Things(37)/Datastreams(84)/Observations?$select=result,%20phenomenonTime&$filter=phenomenonTime%20ge%202017-12-30T00:00:00.000Z%20and%20phenomenonTime%20le%202018-01-10T00:00:00.000Z&$resultFormat=dataArray")

da.airTemp <- as.data.frame(airTemp$value$dataArray[[1]])
da.airTemp$V1 <- ymd_hms(da.airTemp$V1)
da.airTemp$V2 <- as.numeric(paste(da.airTemp$V2))
names(da.airTemp) <- c('DateTime', 'AirTemperature')

airHumid <- fromJSON("https://beaware.server.de/SensorThingsService/v1.0/Things(37)/Datastreams(117)/Observations?$select=result,%20phenomenonTime&$filter=phenomenonTime%20ge%202016-01-01T00:00:00.000Z%20and%20phenomenonTime%20le%202016-05-01T00:00:00.000Z&$resultFormat=dataArray")

da.airHumid <- as.data.frame(airHumid$value$dataArray[[1]])
da.airHumid$V1 <- ymd_hms(da.airHumid$V1)
da.airHumid$V2 <- as.numeric(paste(da.airHumid$V2))
names(da.airHumid) <- c('DateTime', 'AirHumidity')


precip <- fromJSON("https://beaware.server.de/SensorThingsService/v1.0/Things(37)/Datastreams(148)/Observations?$select=result,%20phenomenonTime&$filter=phenomenonTime%20ge%202016-01-01T00:00:00.000Z%20and%20phenomenonTime%20le%202016-05-01T00:00:00.000Z&$resultFormat=dataArray")

da.precip <- as.data.frame(precip$value$dataArray[[1]])
da.precip$V1 <- ymd_hms(da.precip$V1)
da.precip$V2 <- as.numeric(paste(da.precip$V2))
names(da.precip) <- c('DateTime', 'Precipitation')

# da.all <- data.frame( rbind(da.airTemp, da.airHumid, da.precip ) )
# names(da.all) <- c('DateTime', 'AirTemperature','AirHumidity', 'Precipitation')
# 
# 
# p <- ggplot( da.all ) + geom_line(aes(x=DateTime, y=AirTemperature))  
# p <- p + geom_line(aes(x=DateTime, y=AirHumidity, col='blue')) 
# p <- p + geom_line(aes(x=DateTime, y=Precipitation, ))
# plot(p)



  # myfile <- fromJSON('https://beaware.server.de/SensorThingsService/v1.0/Observations?$resultFormat=dataArray')
  # 
  # a <- as.data.table(myfile[["value"]]$dataArray)
  # names_a <- c("id", "phenomenonTime" ,"result" , "resultTime", "resultQuality" , "validTime" ,     "parameters")
  # colnames(a) <- names_a
  # 
  # 

  # 
  # q1 <- fromJSON("https://beaware.server.de/SensorThingsService/v1.0/Things(390)$resultFormat=DataArray")
  # 
  # q1 <- fromJSON('https://beaware.server.de/SensorThingsService/v1.0/Things(390)?$expand=Datastreams($select=id,name,properties;$expand=Observations)')
  # 
  # q2 <- fromJSON('https://beaware.server.de/SensorThingsService/v1.0/Datastreams(496)/Observations')

  

  
#------------------------------------------------------------
my_api_key <- 'f96cb70b-64d1-4bbc-9044-283f62a8c734'

fmi.addr <- 'http://data.fmi.fi/fmi-apikey'

getCapabilities <- paste( fmi.addr,  my_api_key, 'wfs?request=getCapabilities', sep="/")


storedQueries <- paste(fmi.addr, my_api_key, 'wfs?request=describeStoredQueries', sep="/")


q <- paste(fmi.addr, my_api_key, 'wfs?request=getFeature&storedquery_id=fmi::forecast::hirlam::surface::point::multipointcoverage&place=helsinki', sep="/")


#--------------------------- HIRLAM RCR MODEL FORECAST
# it does not work
#surf_query <- paste( fmi.addr,  my_api_key, 'wfs?\request=getFeature\\&storedquery_id=fmi::forecast::hirlam::surface::grid', sep="/" )




library("XML")

# LOADING TRANSFORMED XML INTO R DATA FRAME
doc <- xmlParse(file=q, isURL = TRUE)
xmldf <- xmlToDataFrame(doc)
View(xmldf)

d <- xmlTreeParse(q)



