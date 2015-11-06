package ch.simplatyser.elastic.housekeeping;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.Collections;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by knobli on 28.10.2015.
 */
public class HousekeepingServiceTest {

    @Test
    @Ignore
    public void runHousekeeping() throws Exception {
        HousekeepingService service = new HousekeepingService();
        service.setElasticSearchHost("ES Host");
        service.setElasticSearchPort(9300);
        service.setElasticSearchCluster("clustername");
        String indexPattern = "logstash";
        int leaveDays = 999;
        service.runHousekeeping(Collections.singletonList(new HousekeepingEntry(indexPattern, leaveDays)));
    }

    @Test
    @Ignore
    public void testGetIndices() throws Exception {
        HousekeepingService service = new HousekeepingService();
        service.setElasticSearchHost("ES Host");
        service.setElasticSearchPort(9300);
        service.setElasticSearchCluster("clustername");
        List<ElasticIndex> indices = service.getIndices("logstash-");
        assertThat(indices).isNotEmpty();
    }
}