package ch.simplatyser.elastic.housekeeping;

/**
 * Created by r.santschi on 06.11.2015.
 */
public class ElasticIndex {
    public String name;
    public Long size;

    public ElasticIndex(String name, Long size) {
        this.name = name;
        this.size = size;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }
}
