package ch.simplatyser.elastic.housekeeping;

/**
 * Created by r.santschi on 03.11.2015.
 */
public class HousekeepingEntry {
    private String indexPattern;
    private int leaveDays;

    public HousekeepingEntry(){}

    public HousekeepingEntry(String indexPattern, int leaveDays) {
        this.indexPattern = indexPattern;
        this.leaveDays = leaveDays;
    }

    public String getIndexPattern() {
        return indexPattern;
    }

    public void setIndexPattern(String indexPattern) {
        this.indexPattern = indexPattern;
    }

    public int getLeaveDays() {
        return leaveDays;
    }

    public void setLeaveDays(int leaveDays) {
        this.leaveDays = leaveDays;
    }
}
