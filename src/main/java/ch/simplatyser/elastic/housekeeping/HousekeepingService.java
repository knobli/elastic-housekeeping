package ch.simplatyser.elastic.housekeeping;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by knobli on 27.10.2015.
 */
public class HousekeepingService {

    private final Logger log = LoggerFactory.getLogger(HousekeepingService.class);

    private static String elasticSearchHost = "localhost";
    private static int elasticSearchPort = 9300;

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Invalid number of arguments(" + args.length + "), should be 2");
            System.exit(1);
        }
        String indexPattern = String.valueOf(args[0]);
        if (indexPattern == null) {
            System.err.println("Invalid index pattern");
            System.exit(1);
        }
        int leaveDays = 999;
        try {
            leaveDays = Integer.parseInt(args[1]);
        } catch (NumberFormatException e) {
            System.err.println("Invalid leave days number");
            System.exit(1);
        }
        if (System.getProperties().getProperty("elasticSearchHost") != null) {
            elasticSearchHost = System.getProperties().getProperty("elasticSearchHost");
        }
        if (System.getProperties().getProperty("elasticSearchPort") != null) {
            try {
                elasticSearchPort = Integer.parseInt(System.getProperties().getProperty("elasticSearchPort"));
            } catch (NumberFormatException e) {
                System.err.println("Invalid port number");
                System.exit(1);
            }
        }

        HousekeepingService service = new HousekeepingService();
        service.runHousekeeping(indexPattern, leaveDays);
    }

    public void runHousekeeping(String indexPattern, int leaveDays) {
        if (indexPattern.substring(indexPattern.length() - 1).equals("-")) {
            log.error("Invalid index pattern, the pattern should not end with '-'");
            return;
        }
        log.info("Index pattern: " + indexPattern);
        log.info("Leave days: " + leaveDays);
        log.info("Host: " + elasticSearchHost);
        log.info("Port: " + elasticSearchPort);
        List<String> keepIndices = new ArrayList<>();
        for (int i = 0; i < leaveDays; i++) {
            DateTime currentDate = DateTime.now().minusDays(i);
            keepIndices.add(indexPattern + ".*\\-" + currentDate.toString("YYYY\\.MM\\.dd"));
        }
        List<String> availableIndices = getIndices(indexPattern);
        log.info("Found " + availableIndices.size() + " indices with pattern " + indexPattern);
        int deleted = 0;
        for (String availableIndex : availableIndices) {
            boolean keep = false;
            for (String keepIndex : keepIndices) {
                if (availableIndex.matches(keepIndex)) {
                    keep = true;
                }
            }
            if (!keep) {
                log.info("Delete index: " + availableIndex);
                removeIndex(availableIndex);
                deleted++;
            }
        }
        log.info("Deleted " + deleted + " indices with pattern " + indexPattern);
    }


    public void removeIndex(String indexName) {
        TransportClient client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(elasticSearchHost, elasticSearchPort));
        DeleteIndexResponse response = client.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
        if (response.isAcknowledged()) {
            log.info("Index " + indexName + " successfully deleted.");
        } else {
            log.error("Could not delete index " + indexName);
        }
    }

    public List<String> getIndices(String indexPattern) {
        List<String> indicesAsString = new ArrayList<>();
        TransportClient client = new TransportClient().addTransportAddress(new InetSocketTransportAddress(elasticSearchHost, elasticSearchPort));
        ImmutableOpenMap<String, IndexMetaData> indices = client.admin().cluster()
                .prepareState().execute()
                .actionGet().getState().getMetaData()
                .getIndices();
        for (ObjectObjectCursor<String, IndexMetaData> index : indices) {
            if (index.key.contains(indexPattern)) {
                indicesAsString.add(index.key);
            }
        }
        return indicesAsString;
    }

}
