package ch.simplatyser.elastic.housekeeping;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
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
            LOGGER.info("Index pattern: {}, leave day: {}, host: {}, port: {}, cluster: {}", indexPattern, leaveDays, elasticSearchHost, elasticSearchPort, elasticSearchCluster);
            List<String> keepIndices = new ArrayList<>();
            for (int i = 0; i < leaveDays; i++) {
                DateTime currentDate = DateTime.now().minusDays(i);
                keepIndices.add(indexPattern + ".*\\-" + currentDate.toString("YYYY\\.MM\\.dd"));
            }
            List<ElasticIndex> availableIndices = getIndices(indexPattern);
            long totalSize = calcTotalSize(availableIndices);
            LOGGER.info("Found " + availableIndices.size() + " indices with pattern " + indexPattern + " with total size of " + humanReadableByteCount(totalSize, false));
            int deleted = 0;
            int cleanupSize = 0;
            for (ElasticIndex availableIndex : availableIndices) {
                boolean keep = false;
                for (String keepIndex : keepIndices) {
                    if (availableIndex.getName().matches(keepIndex)) {
                        keep = true;
                    }
                }
                if (!keep) {
                    if(removeIndex(availableIndex)) {
                        cleanupSize += availableIndex.getSize();
                        deleted++;
                    }
                }
            }
            LOGGER.info("Deleted " + deleted + " indices with pattern " + indexPattern + " and save " + humanReadableByteCount(cleanupSize, false) + " of storage.");
        }
    }

    private long calcTotalSize(List<ElasticIndex> availableIndices) {
        long totalSize = 0;
        for (ElasticIndex availableIndex : availableIndices) {
            totalSize += availableIndex.getSize();
        }
        return totalSize;
    }


    public boolean removeIndex(ElasticIndex index) {
        TransportClient client = createTransportClient();
        if (client != null) {
            try {
                String indexName = index.getName();
                DeleteIndexResponse response = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
                if (response.isAcknowledged()) {
                    LOGGER.info("Index " + indexName + " successfully deleted.");
                    return true;
                } else {
                    LOGGER.error("Could not delete index " + indexName);
                }
            } catch (ElasticsearchException e) {
                LOGGER.error("Error during index request", e);
            }
        }
        return false;
    }

    private TransportClient createTransportClient() {
        try {
            InetAddress host = InetAddress.getByName(elasticSearchHost);
            Settings settings = Settings.builder().put("cluster.name", elasticSearchCluster).build();
            TransportClient client = new PreBuiltTransportClient(settings);
            client.addTransportAddress(new InetSocketTransportAddress(host, elasticSearchPort));
            return client;
        } catch (UnknownHostException e) {
            LOGGER.error("Could not get host '" + elasticSearchHost + "'", e);
        }
        return null;
    }

    public List<ElasticIndex> getIndices(String indexPattern) {
        List<ElasticIndex> indicesAsString = new ArrayList<>();
        TransportClient client = createTransportClient();
        if (client != null) {
            try {
                IndicesStatsResponse stats = client.admin().indices().prepareStats()
                        .clear()
                        .setStore(true)
                        .execute().actionGet();
                ImmutableOpenMap<String, IndexMetaData> indices = client.admin().cluster().prepareState().execute()
                        .actionGet().getState().getMetaData()
                        .getIndices();
                for (Map.Entry<String, IndexStats> index : stats.getIndices().entrySet()) {
                    if (index.getKey().contains(indexPattern)) {
                        long size = stats.getIndex(index.getKey()).getTotal().getStore().getSizeInBytes();
                        indicesAsString.add(new ElasticIndex(index.getKey(), size));
                    }
                }
            } catch (ElasticsearchException e) {
                LOGGER.error("Error during index request", e);
            }
        }
        return indicesAsString;
    }

    public static String humanReadableByteCount(long bytes, boolean si) {
        int unit = si ? 1000 : 1024;
        if (bytes < unit) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(unit));
        String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
        return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
    }

    public static void setElasticSearchHost(String elasticSearchHost) {
        HousekeepingService.elasticSearchHost = elasticSearchHost;
    }

    public static void setElasticSearchPort(int elasticSearchPort) {
        HousekeepingService.elasticSearchPort = elasticSearchPort;
    }

    public static void setElasticSearchCluster(String elasticSearchCluster) {
        HousekeepingService.elasticSearchCluster = elasticSearchCluster;
    }
}
