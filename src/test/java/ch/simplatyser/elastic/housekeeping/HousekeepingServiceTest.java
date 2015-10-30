package ch.simplatyser.elastic.housekeeping;

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
        String indexPattern = "logstash-technicala";
        int leaveDays = 30;
        service.runHousekeeping(indexPattern, leaveDays);
    }

    @Test
    @Ignore
    public void testGetIndices() throws Exception {
        HousekeepingService service = new HousekeepingService();
        List<String> indices = service.getIndices("logstash-");
    }
}