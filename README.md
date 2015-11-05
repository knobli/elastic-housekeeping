# elastic-housekeeping
Elastic 2.0.0 Index Housekeeping

# Build
mvn clean package

# Elastic search configuration
* Hostname: -DelasticSearchHost (default: localhost)
* Port: -DelasticSearchPort (default: 9300)
* Cluster: -DelasticSearchCluster (no default)

# Start with command line parameter
<pre>
java -jar -DelasticSearchHost=172.9.9.1 -DelasticSearchPort=9900 -DelasticSearchCluster=test elastic-housekeeping.jar "logstash-staging" 20
</pre>

* first param: index pattern
* second param: leave days

# Start with properties file
<pre>
java -jar -DelasticSearchHost=172.9.9.1 -DelasticSearchPort=9900 -DelasticSearchCluster=test elastic-housekeeping.jar "FULL_PATH_TO_PROPERTIES_FILE"
</pre>

## Properties file
<pre>
INDEX_PATTERN.1=logstash-technicala
LEAVE_TIME.1=30
INDEX_PATTERN.2=logstash-technicalb
LEAVE_TIME.2=99
</pre>

# Logs
/var/log/elastic-housekeeping/housekeeping.log

# Using cronjob
<pre>
# m h dom mon dow user  command
0  3    * * *   root    java -jar -DelasticSearchCluster=test /opt/elastic-housekeeping/elastic-housekeeping.jar /opt/elastic-housekeeping/housekeeping.properties
</pre>

