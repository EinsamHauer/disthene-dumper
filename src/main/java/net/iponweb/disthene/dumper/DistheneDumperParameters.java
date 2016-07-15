package net.iponweb.disthene.dumper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Andrei Ivanov
 */
public class DistheneDumperParameters {
//  -ll DEBUG -o /home/aivanov/work/_____deleteme/dump -c cassandra-1b.graphite.devops.iponweb.net -e es-1a.graphite.devops.iponweb.net

    private String outputLocation;
    private long startTime;
    private long endTime;
    private List<Rollup> rollups = new ArrayList<>();
    private String cassandraContactPoint;
    private String elasticSearchContactPoint;
    private int threads;
    private Set<String> cassandraBlacklist = new HashSet<>();


    public String getOutputLocation() {
        return outputLocation;
    }

    public void setOutputLocation(String outputLocation) {
        this.outputLocation = outputLocation;
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public void addRollup(String rollupString) {
        rollups.add(new Rollup(rollupString));
    }

    public List<Rollup> getRollups() {
        return rollups;
    }

    public String getCassandraContactPoint() {
        return cassandraContactPoint;
    }

    public void setCassandraContactPoint(String cassandraContactPoint) {
        this.cassandraContactPoint = cassandraContactPoint;
    }

    public String getElasticSearchContactPoint() {
        return elasticSearchContactPoint;
    }

    public void setElasticSearchContactPoint(String elasticSearchContactPoint) {
        this.elasticSearchContactPoint = elasticSearchContactPoint;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }

    public Set<String> getCassandraBlacklist() {
        return cassandraBlacklist;
    }

    public void addToCassandraBlacklist(String node) {
        cassandraBlacklist.add(node);
    }

    @Override
    public String toString() {
        return "DistheneDumperParameters{" +
                "outputLocation='" + outputLocation + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", rollups=" + rollups +
                ", cassandraContactPoint='" + cassandraContactPoint + '\'' +
                ", elasticSearchContactPoint='" + elasticSearchContactPoint + '\'' +
                ", threads=" + threads +
                '}';
    }
}
