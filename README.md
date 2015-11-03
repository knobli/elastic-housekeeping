# elastic-housekeeping
Elastic Index Housekeeping

# Build
mvn clean package

# Start with command line parameter
java -jar "-DelasticSearchHost=172.9.9.1" elastic-housekeeping.jar "logstash-staging" 20

first param: index pattern

second param: leave days

# Start with properties file
java -jar "-DelasticSearchHost=172.9.9.1" elastic-housekeeping.jar "FULL_PATH_TO_PROPERTIES_FILE"

## Properties file
INDEX_PATTERN.1=logstash-technicala
LEAVE_TIME.1=30

# Logs
/var/log/elastic-housekeeping/housekeeping.log

# Using cronjob
