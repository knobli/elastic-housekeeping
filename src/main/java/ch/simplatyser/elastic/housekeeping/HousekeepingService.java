package ch.simplatyser.elastic.housekeeping;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

/**
 * Created by knobli on 27.10.2015.
 */
public class HousekeepingService {

    public static final String INDEX_PATTERN_REGEX = "INDEX_PATTERN\\.(\\d+)";
    public static final String LEAVE_TIME_PATTERN = "LEAVE_TIME.";

    private static final Logger LOGGER = LoggerFactory.getLogger(HousekeepingService.class);

    private static String elasticSearchHost = "localhost";
    private static int elasticSearchPort = 9300;
    private static String elasticSearchCluster = null;

    private List<HousekeepingEntry> housekeepingEntries = new ArrayList<>();

    public static void main(String[] args) {
        HousekeepingService service = new HousekeepingService();
        if (args.length == 1) {
            service.readParametersFromPropertiesFile(args[0]);
        } else if (args.length == 2) {
            service.readParametersFromCommandLine(args);
        } else {
            LOGGER.error("Invalid number of arguments(" + args.length + "), should be 2");
            System.err.println("Invalid number of arguments(" + args.length + "), should be 2");
            System.exit(1);
        }
        if (System.getProperties().getProperty("elasticSearchHost") != null) {
            elasticSearchHost = System.getProperties().getProperty("elasticSearchHost");
        }
        if (System.getProperties().getProperty("elasticSearchPort") != null) {
            try {
                elasticSearchPort = Integer.parseInt(System.getProperties().getProperty("elasticSearchPort"));
            } catch (NumberFormatException e) {
                LOGGER.error("Invalid port number", e);
                System.err.println("Invalid port number");
                System.exit(1);
            }
        }
        elasticSearchCluster = System.getProperties().getProperty("elasticSearchCluster");
        if (elasticSearchCluster == null) {
            LOGGER.error("elasticsearch cluster not set");
            System.err.println("elasticsearch cluster not set (-DelasticSearchCluster)");
            System.exit(1);
        }
        service.startHousekeeping();
    }

    private void readParametersFromPropertiesFile(String propertiesFilePath) {
        if (propertiesFilePath == null) {
            LOGGER.error("Invalid properties file path");
            System.err.println("Invalid properties file path");
            System.exit(1);
        }
        Properties prop = new Properties();
        InputStream input = null;
        try {
            LOGGER.info("Try to read properties file: " + propertiesFilePath);
            input = new FileInputStream(propertiesFilePath);
            // load a properties file
            prop.load(input);
        } catch (IOException ex) {
            LOGGER.error("Could not read properties file");
            System.exit(1);
        } finally {
            if (input != null) {
                try {
                    input.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        for (String property : prop.stringPropertyNames()) {
            if (property.matches(INDEX_PATTERN_REGEX)) {
                Pattern pattern = Pattern.compile(INDEX_PATTERN_REGEX);
                Matcher matcher = pattern.matcher(property);
                if (matcher.find()) {
                    String indexPatternNumber = matcher.group(1);
                    String indexPattern = prop.getProperty(property);
                    int leaveDays;
                    try {
                        leaveDays = Integer.parseInt(prop.getProperty(LEAVE_TIME_PATTERN + indexPatternNumber));
                    } catch (NumberFormatException e) {
                        LOGGER.error("Invalid leave days number for " + property, e);
                        System.err.println("Invalid leave days number for " + property);
                        continue;
                    }
                    housekeepingEntries.add(new HousekeepingEntry(indexPattern, leaveDays));
                }
            }
        }
    }

    private void readParametersFromCommandLine(String[] args) {
        String indexPattern = args[0];
        if (indexPattern == null) {
            LOGGER.error("Invalid index pattern");
            System.err.println("Invalid index pattern");
            System.exit(1);
        }
        int leaveDays = 999;
        try {
            leaveDays = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            LOGGER.error("Invalid leave days number");
            System.err.println("Invalid leave days number");
            System.exit(1);
        }
        housekeepingEntries.add(new HousekeepingEntry(indexPattern, leaveDays));
    }

    public void startHousekeeping() {
        runHousekeeping(housekeepingEntries);
    }

    public void runHousekeeping(List<HousekeepingEntry> housekeepingEntries) {
        for (HousekeepingEntry housekeepingEntry : housekeepingEntries) {
            String indexPattern = housekeepingEntry.getIndexPattern();
            int leaveDays = housekeepingEntry.getLeaveDays();
            if (indexPattern.substring(indexPattern.length() - 1).equals("-")) {
                LOGGER.error("Invalid index pattern, the pattern should not end with '-'");
                continue;
            }
            LOGGER.info("Index pattern: " + indexPattern);
            LOGGER.info("Leave days: " + leaveDays);
            LOGGER.info("Host: " + elasticSearchHost);
            LOGGER.info("Port: " + elasticSearchPort);
            LOGGER.info("Cluster: " + elasticSearchCluster);
            List<String> keepIndices = new ArrayList<>();
            for (int i = 0; i < leaveDays; i++) {
                DateTime currentDate = DateTime.now().minusDays(i);
                keepIndices.add(indexPattern + ".*\\-" + currentDate.toString("YYYY\\.MM\\.dd"));
            }
            List<String> availableIndices = getIndices(indexPattern);
            LOGGER.info("Found " + availableIndices.size() + " indices with pattern " + indexPattern);
            int deleted = 0;
            for (String availableIndex : availableIndices) {
                boolean keep = false;
                for (String keepIndex : keepIndices) {
                    if (availableIndex.matches(keepIndex)) {
                        keep = true;
                    }
                }
                if (!keep) {
                    LOGGER.info("Delete index: " + availableIndex);
                    removeIndex(availableIndex);
                    deleted++;
                }
            }
            LOGGER.info("Deleted " + deleted + " indices with pattern " + indexPattern);
        }
    }


    public void removeIndex(String indexName) {
        TransportClient client = createTransportClient();
        if (client != null) {
            try {
                DeleteIndexResponse response = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
                if (response.isAcknowledged()) {
                    LOGGER.info("Index " + indexName + " successfully deleted.");
                } else {
                    LOGGER.error("Could not delete index " + indexName);
                }
            } catch (ElasticsearchException e) {
                LOGGER.error("Error during index request", e);
            }
        }
    }

    private TransportClient createTransportClient() {
        try {
            InetAddress host = InetAddress.getByName(elasticSearchHost);
            return TransportClient.builder().settings(Settings.builder().put("cluster.name", elasticSearchCluster)).build().
                    addTransportAddress(new InetSocketTransportAddress(host, elasticSearchPort));
        } catch (UnknownHostException e) {
            LOGGER.error("Could not get host '" + elasticSearchHost + "'", e);
        }
        return null;
    }

    public List<String> getIndices(String indexPattern) {
        List<String> indicesAsString = new ArrayList<>();
        TransportClient client = createTransportClient();
        if (client != null) {
            try {
                ImmutableOpenMap<String, IndexMetaData> indices = client.admin().cluster().prepareState().execute()
                        .actionGet().getState().getMetaData()
                        .getIndices();
                for (ObjectObjectCursor<String, IndexMetaData> index : indices) {
                    if (index.key.contains(indexPattern)) {
                        indicesAsString.add(index.key);
                    }
                }
            } catch (ElasticsearchException e) {
                LOGGER.error("Error during index request", e);
            }
        }
        return indicesAsString;
    }

    public static void setElasticSearchHost(String elasticSearchHost) {
        HousekeepingService.elasticSearchHost = elasticSearchHost;
    }

    public static void setElasticSearchPort(int elasticSearchPort) {
        HousekeepingService.elasticSearchPort = elasticSearchPort;
    }

}