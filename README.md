# elastic-housekeeping
Elastic Index Housekeeping

# Build
mvn clean package

# Elastic search configuration
* Hostname: -DelasticSearchHost (default: localhost)
* Port: -DelasticSearchPort (default: 9300)

# Start with command line parameter
java -jar "-DelasticSearchHost=172.9.9.1 -DelasticSearchPort=9900" elastic-housekeeping.jar "logstash-staging" 20

* first param: index pattern
* second param: leave days

# Start with properties file
java -jar "-DelasticSearchHost=172.9.9.1 -DelasticSearchPort=9900" elastic-housekeeping.jar "FULL_PATH_TO_PROPERTIES_FILE"

## Properties file
<code>
INDEX_PATTERN.1=logstash-technicala

LEAVE_TIME.1=30

INDEX_PATTERN.2=logstash-technicalb

LEAVE_TIME.2=99
</code>

# Logs
/var/log/elastic-housekeeping/housekeeping.log

# Using cronjob
