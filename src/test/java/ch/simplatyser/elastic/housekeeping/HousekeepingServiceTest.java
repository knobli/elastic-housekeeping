package ch.simplatyser.elastic.housekeeping;

import static org.fest.assertions.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.joda.time.LocalDateTime;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Created by knobli on 28.10.2015.
 */
public class HousekeepingServiceTest {

    private List<ElasticIndex> indices;
    private List<String> removedIndices = new ArrayList<>();

    private HousekeepingService service = new HousekeepingService(){

        @Override
        public List<ElasticIndex> getIndices(String indexPattern) {
            return indices;
        }

        @Override
        public boolean removeIndex(ElasticIndex index) {
            removedIndices.add(index.getName());
            return true;
        }
    };

    @Test
    public void runHousekeeping() throws Exception {
        indices = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        indices.add(new ElasticIndex("logstash-technicala-" + now.minusDays(5).toString("YYYY.MM.dd"), 1000l));
        indices.add(new ElasticIndex("logstash-technicala-" + now.minusDays(4).toString("YYYY.MM.dd"), 1000l));
        indices.add(new ElasticIndex("logstash-technicala-" + now.minusDays(3).toString("YYYY.MM.dd"), 1000l));
        indices.add(new ElasticIndex("logstash-technicala-" + now.minusDays(2).toString("YYYY.MM.dd"), 1000l));
        indices.add(new ElasticIndex("logstash-technicala-" + now.minusDays(1).toString("YYYY.MM.dd"), 1000l));
        indices.add(new ElasticIndex("logstash-technicala-" + now.toString("YYYY.MM.dd"), 1000l));

        String indexPattern = "logstash-technical";
        int leaveDays = 4;
        service.runHousekeeping(Collections.singletonList(new HousekeepingEntry(indexPattern, leaveDays)));

        assertThat(removedIndices).containsOnly("logstash-technicala-" + now.minusDays(5).toString("YYYY.MM.dd"), "logstash-technicala-" + now.minusDays(4).toString("YYYY.MM.dd"));
    }

    @Test
    public void runHousekeeping_reindexing() throws Exception {
        indices = new ArrayList<>();
        LocalDateTime now = LocalDateTime.now();
        indices.add(new ElasticIndex("logstash-technicala-b-" + now.minusDays(5).toString("YYYY.MM.dd"), 1000l));
        indices.add(new ElasticIndex("logstash-technicala-b-" + now.minusDays(4).toString("YYYY.MM.dd"), 1000l));
        indices.add(new ElasticIndex("logstash-technicala-b-" + now.minusDays(3).toString("YYYY.MM.dd"), 1000l));
        indices.add(new ElasticIndex("logstash-technicala-b-" + now.minusDays(2).toString("YYYY.MM.dd"), 1000l));
        indices.add(new ElasticIndex("logstash-technicala-" + now.minusDays(1).toString("YYYY.MM.dd"), 1000l));
        indices.add(new ElasticIndex("logstash-technicala-" + now.toString("YYYY.MM.dd"), 1000l));

        String indexPattern = "logstash-technical";
        int leaveDays = 4;
        service.runHousekeeping(Collections.singletonList(new HousekeepingEntry(indexPattern, leaveDays)));

        assertThat(removedIndices).containsOnly("logstash-technicala-b-" + now.minusDays(5).toString("YYYY.MM.dd"), "logstash-technicala-b-" + now.minusDays(4).toString("YYYY.MM.dd"));
    }

}