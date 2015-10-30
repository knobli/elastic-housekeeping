# elastic-housekeeping
Elastic Index Housekeeping

# Build
mvn clean package

# Start
java -jar "-DelasticSearchHost=172.9.9.1" elastic-housekeeping.jar "logstash-staging" 20

first param: index pattern\
second param: leave days

## Logs
/var/log/elastic-housekeeping/housekeeping.log

## Using cronjob
