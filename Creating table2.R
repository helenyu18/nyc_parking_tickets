#Connect PostgreSQL with R studio

require('RPostgreSQL')
require('DBI')

drv = dbDriver('PostgreSQL')

#Before you write the con code below, you should create sql_project in pgAdmin
con<- dbConnect(drv, dbname = 'sql_project4',
                host = 'localhost', port = 5432,
                user = 'postgres', password = '')

##Create tables
stmt = "
CREATE TABLE plate_type(
plate_type_code integer,
plate_type varchar(10),
PRIMARY KEY (plate_type_code)
);

CREATE TABLE vehicle_bodies(
vehicle_body_code integer,
vehicle_body_type varchar(10),
PRIMARY KEY (vehicle_body_code)
);

CREATE TABLE vehicle_makes (
vehicle_make_code integer,
vehicle_make varchar(20),
PRIMARY KEY (vehicle_make_code)
);

CREATE TABLE colors (
color_code integer,
color varchar(15),
PRIMARY KEY (color_code)
);

CREATE TABLE law (
law_code integer,
law_section integer,
sub_division varchar(10),
PRIMARY KEY (law_code)
);

CREATE TABLE days_parking_effect (
effect_code integer,
days_parking_in_effect varchar(20),
from_hours_in_effect varchar(10),
to_hours_in_effect varchar(10),
PRIMARY KEY (effect_code)
);

CREATE TABLE issuers (
issuer_id integer, 
issuer_code integer,
issuer_precinct integer,
issuer_command varchar(10),
issuer_squad varchar(10),
PRIMARY KEY (issuer_id)
);

CREATE TABLE descriptions (
description_code integer,
violation_description varchar(100),
PRIMARY KEY (description_code)
);

CREATE TABLE locations (
location_code integer,
violation_location integer,
county varchar(10),
house_number integer,
street_name varchar(100),
intersecting_street varchar(100),
PRIMARY KEY (location_code)
);

CREATE TABLE plates (
plate_code integer, 
plate_id varchar(10),
state varchar(2),
plate_type_code integer,
vehicle_expiration date,
unregistered_vehicle integer,
PRIMARY KEY (plate_code),
FOREIGN KEY (plate_type_code) REFERENCES plate_type (plate_type_code)
);

CREATE TABLE vehicles (
vehicle_code integer,
vehicle_body_code integer,
vehicle_make_code integer,
color_code integer,
vehicle_year integer,
PRIMARY KEY (vehicle_code),
FOREIGN KEY (vehicle_body_code) REFERENCES vehicle_bodies (vehicle_body_code),
FOREIGN KEY (vehicle_make_code) REFERENCES vehicle_makes (vehicle_make_code),
FOREIGN KEY (color_code) REFERENCES colors (color_code)
);

CREATE TABLE violations (
summons_number varchar(15),
plate_code integer,
issue_date date,
violation_code integer,
issuing_agent varchar(5),
location_code integer,
violation_precinct integer,
violation_time varchar(5),
time_first_observed varchar(5),
violation_in_front_of_or_opposite varchar(5),
date_first_observed integer,
violation_legal_code varchar(5),
effect_code integer,
meter_number integer,
feet_from_curb integer,
violation_post_code integer,
description_code integer,
PRIMARY KEY (summons_number),
FOREIGN KEY (plate_code) REFERENCES plates (plate_code),
FOREIGN KEY (location_code) REFERENCES locations (location_code),
FOREIGN KEY (effect_code) REFERENCES days_parking_effect (effect_code),
FOREIGN KEY (description_code) REFERENCES descriptions (description_code)
);

CREATE TABLE vehicle_plates (
vehicle_code integer,
plate_code integer,
PRIMARY KEY (vehicle_code, plate_code),
FOREIGN KEY (vehicle_code) REFERENCES vehicles (vehicle_code),
FOREIGN KEY (plate_code) REFERENCES plates (plate_code)
);

CREATE TABLE violation_issuer (
summons_number varchar(15),
issuer_id integer,
PRIMARY KEY (summons_number, issuer_id),
FOREIGN KEY (summons_number) REFERENCES violations (summons_number),
FOREIGN KEY (issuer_id) REFERENCES issuers (issuer_id)
);

CREATE TABLE violation_info (
summons_number varchar(15),
law_code integer,
PRIMARY KEY (summons_number, law_code),
FOREIGN KEY (summons_number) REFERENCES violations (summons_number),
FOREIGN KEY (law_code) REFERENCES law (law_code)
);
"

# Execute the statement to create tables
dbGetQuery(con, stmt)

table <- read.csv('Parking_Violations_Issued_-_Fiscal_Year_2017.csv')

table<-table[1:2000,]

names(table)<-tolower(names(table))

#plate_type
plate_type_table <- data.frame('plate_type' = unique(table$plate.type))
plate_type_table$plate_type_code <- 1:nrow(plate_type_table)
dbWriteTable(con, name="plate_type", value=plate_type_table, row.names=FALSE, append=TRUE)
#Map and add plate_type_code to the main frame
plate_type_code_list <- sapply(table$plate.type, function(x) plate_type_table$plate_type_code[plate_type_table$plate_type == x])
table$plate_type_code<-plate_type_code_list


#vehicle_bodies
vehicle_bodies_table <- data.frame('vehicle_body_type' = unique(table$vehicle.body.type))
vehicle_bodies_table$vehicle_body_code <- 1:nrow(vehicle_bodies_table)
dbWriteTable(con, name="vehicle_bodies", value=vehicle_bodies_table, row.names=FALSE, append=TRUE)
#Map and add vehicle_body_code to the main frame
vehicle_body_code_list <- sapply(table$vehicle.body.type, function(x) vehicle_bodies_table$vehicle_body_code[vehicle_bodies_table$vehicle_body_type == x])
table$vehicle_body_code<-vehicle_body_code_list


#vehicle_makes
vehicle_makes_table <- data.frame('vehicle_make' = unique(table$vehicle.make))
vehicle_makes_table$vehicle_make_code <- 1:nrow(vehicle_makes_table)
dbWriteTable(con, name="vehicle_makes", value=vehicle_makes_table, row.names=FALSE, append=TRUE)
#Map and add vehicle_make_code to the main frame
vehicle_make_code_list <- sapply(table$vehicle.make, function(x) vehicle_makes_table$vehicle_make_code[vehicle_makes_table$vehicle_make == x])
table$vehicle_make_code<-vehicle_make_code_list


#colors
colors_table <- data.frame('color' = unique(table$vehicle.color))
colors_table$color_code <- 1:nrow(colors_table)
dbWriteTable(con, name="colors", value=colors_table, row.names=FALSE, append=TRUE)
#Map and add color_code to the main frame
color_code_list <- sapply(table$vehicle.color, function(x) colors_table$color_code[colors_table$color == x])
table$color_code<-color_code_list


#law
#Combine law.section and sub.division into a new variable "law_sub" to ensure the uniqueness
law_sub_table<-data.frame('law_sub'= rowSums(cbind(table$law.section,table$sub.division),na.rm = TRUE))
table$law_sub<-law_sub_table$law_sub
law_table <- data.frame('law_sub' = unique(table$law_sub))
law_table$law_code <- 1:nrow(law_table)
#Map and add law_code to the main frame
law_code_list <- sapply(table$law_sub, function(x) law_table$law_code[law_table$law_sub == x])
table$law_code<-law_code_list
#Change column names to match the schema
names(table)[28]<-"law_section"
names(table)[29]<-"sub_division"
dbWriteTable(con, name="law", value=table[c('law_section','sub_division','law_code')][!duplicated(table[c('law_code')]),], row.names=FALSE, append=TRUE)


#days_parking_effect
#Combine days.parking.in.effect, from.hours.in.effect and to.hours.in.effect into a new variable "effect" to ensure the uniqueness
effect_table<-data.frame('effect'= rowSums(cbind(table$days.parking.in.effect,table$from.hours.in.effect,table$to.hours.in.effect),na.rm = TRUE))
table$effect<-effect_table$effect
effect_code_table <- data.frame('effect' = unique(table$effect))
effect_code_table$effect_code <- 1:nrow(effect_code_table)
#Map and add effect_code to the main frame
effect_code_list <- sapply(table$effect, function(x) effect_code_table$effect_code[effect_code_table$effect == x])
table$effect_code<-effect_code_list
#Change column names to match the schema
names(table)[31]<-"days_parking_in_effect"
names(table)[32]<-"from_hours_in_effect"
names(table)[33]<-"to_hours_in_effect"
dbWriteTable(con, name="days_parking_effect", value=table[c('days_parking_in_effect','from_hours_in_effect','to_hours_in_effect','effect_code')][!duplicated(table[c('effect_code')]),], row.names=FALSE, append=TRUE)


#issuers
#Combine issuer.precinct, issuer.code, issuer.command and issuer.squad into a new variable "issuer" to ensure the uniqueness
issuers_table<-data.frame('issuer'= rowSums(cbind(table$issuer.precinct,table$issuer.code,table$issuer.command,table$issuer.squad),na.rm = TRUE))
table$issuer<-issuers_table$issuer
issuer_id_table <- data.frame('issuer' = unique(table$issuer))
issuer_id_table$issuer_id <- 1:nrow(issuer_id_table)
#Map and add issuer_id to the main frame
issuer_id_list <- sapply(table$issuer, function(x) issuer_id_table$issuer_id[issuer_id_table$issuer == x])
table$issuer_id<-issuer_id_list
#Change column names to match the schema
names(table)[16]<-"issuer_precinct"
names(table)[17]<-"issuer_code"
names(table)[18]<-"issuer_command"
names(table)[19]<-"issuer_squad"
dbWriteTable(con, name="issuers", value=table[c('issuer_precinct','issuer_code','issuer_command','issuer_squad','issuer_id')][!duplicated(table[c('issuer_id')]),], row.names=FALSE, append=TRUE)


#descriptions
descriptions_table <- data.frame('violation_description' = unique(table$violation.description))
descriptions_table$description_code <- 1:nrow(descriptions_table)
dbWriteTable(con, name="descriptions", value=descriptions_table, row.names=FALSE, append=TRUE)
#Map and add description_code to the main frame
description_code_list <- sapply(table$violation.description, function(x) descriptions_table$description_code[descriptions_table$violation_description == x])
table$description_code<-description_code_list

#locations
#Combine violation.location, violation.county, house.number, street.name and intersecting.street into a new variable "location" to ensure the uniqueness
locations_table<-data.frame('location'= rowSums(cbind(table$violation.location,table$violation.county,table$house.number,table$street.name,table$intersecting.street),na.rm = TRUE))
table$location<-locations_table$location
location_code_table <- data.frame('location' = unique(table$location))
location_code_table$location_code <- 1:nrow(location_code_table)
#Map and add location_code to the main frame
location_code_list <- sapply(table$location, function(x) location_code_table$location_code[location_code_table$location == x])
table$location_code<-location_code_list
#Change column names to match the schema
names(table)[14]<-"violation_location"
names(table)[22]<-"county"
names(table)[24]<-"house_number"
names(table)[25]<-"street_name"
names(table)[26]<-"intersecting_street"
#Change the data type of house_number
stmt2 = "alter table locations drop column house_number;
         alter table locations add house_number varchar(20);"
dbGetQuery(con, stmt2)
dbWriteTable(con, name="locations", value=table[c('violation_location','county','house_number','street_name','intersecting_street','location_code')][!duplicated(table[c('location_code')]),], row.names=FALSE, append=TRUE)


#plates
#Combine plate.id, registration.state and plate_type_code into a new variable "plate" to ensure the uniqueness
plates_table<-data.frame('plate'= rowSums(cbind(table$plate.id,table$registration.state,table$plate_type_code),na.rm = TRUE))
table$plate<-plates_table$plate
plate_code_table <- data.frame('plate' = unique(table$plate))
plate_code_table$plate_code <- 1:nrow(plate_code_table)
#Map and add plate_code to the main frame
plate_code_list <- sapply(table$plate, function(x) plate_code_table$plate_code[plate_code_table$plate == x])
table$plate_code<-plate_code_list
#Change column names to match the schema
names(table)[2]<-"plate_id"
names(table)[3]<-"state"
names(table)[13]<-"vehicle_expiration"
names(table)[35]<-"unregistered_vehicle"
#Change the data type of vehicle_expiration
stmt3 = "alter table plates drop column vehicle_expiration;
         alter table plates add vehicle_expiration integer;"
dbGetQuery(con, stmt3)
dbWriteTable(con, name="plates", value=table[c('plate_id','state','plate_type_code','vehicle_expiration','unregistered_vehicle','plate_code')][!duplicated(table[c('plate_code')]),], row.names=FALSE, append=TRUE)


#vehicles
#Combine vehicle_body_code, vehicle_make_code, color_code and vehicle.year into a new variable "vehicle" to ensure the uniqueness
vehicle_table<-data.frame('vehicle'= rowSums(cbind(table$vehicle_body_code,table$vehicle_make_code,table$color_code,table$vehicle.year),na.rm = TRUE))
table$vehicle<-vehicle_table$vehicle
vehicle_code_table <- data.frame('vehicle' = unique(table$vehicle))
vehicle_code_table$vehicle_code <- 1:nrow(vehicle_code_table)
#Map and add vehicle_code to the main frame
vehicle_code_list <- sapply(table$vehicle, function(x) vehicle_code_table$vehicle_code[vehicle_code_table$vehicle == x])
table$vehicle_code<-vehicle_code_list
#Change column names to match the schema
names(table)[36]<-"vehicle_year"
dbWriteTable(con, name="vehicles", value=table[c('vehicle_code','vehicle_body_code','vehicle_make_code','color_code','vehicle_year')][!duplicated(table[c('vehicle_code')]),], row.names=FALSE, append=TRUE)


#violations
#Change column names to match the schema
names(table)[1]<-"summons_number"
names(table)[5]<-"issue_date"
names(table)[6]<-"violation_code"
names(table)[9]<-"issuing_agent"
names(table)[15]<-"violation_precinct"
names(table)[20]<-"violation_time"
names(table)[21]<-"time_first_observed"
names(table)[23]<-"violation_in_front_of_or_opposite"
names(table)[27]<-"date_first_observed"
names(table)[30]<-"violation_legal_code"
names(table)[37]<-"meter_number"
names(table)[38]<-"feet_from_curb"
names(table)[39]<-"violation_post_code"

violations_table<-data.frame(table[c('summons_number','plate_code','issue_date',
                                     'violation_code','issuing_agent','location_code',
                                     'violation_precinct','violation_time','time_first_observed',
                                     'violation_in_front_of_or_opposite','date_first_observed',
                                     'violation_legal_code','effect_code','meter_number',
                                     'feet_from_curb','violation_post_code','description_code')])
#Change the data type of meter_number and violation_post_code
stmt4 = "alter table violations drop column meter_number;
         alter table violations add meter_number varchar(15);
         alter table violations drop column violation_post_code;
         alter table violations add violation_post_code varchar(10);"
dbGetQuery(con, stmt4)
dbWriteTable(con, name="violations", value=violations_table, row.names=FALSE, append=TRUE)

#vehicle_plates
dbWriteTable(con, name="vehicle_plates", value=table[c('vehicle_code','plate_code')][!duplicated(table[c('vehicle_code','plate_code')]),], row.names=FALSE, append=TRUE)

#violation_issuer
dbWriteTable(con, name="violation_issuer", value=table[c('summons_number','issuer_id')][!duplicated(table[c('summons_number','issuer_id')]),], row.names=FALSE, append=TRUE)

#violation_info
dbWriteTable(con, name="violation_info", value=table[c('summons_number','law_code')][!duplicated(table[c('summons_number','law_code')]),], row.names=FALSE, append=TRUE)

