package net.iponweb.disthene.dumper;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
import com.datastax.driver.core.policies.WhiteListPolicy;
import com.google.common.util.concurrent.*;
import org.apache.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * @author Andrei Ivanov
 */
public class Dumper {
    private static final String INDEX_NAME = "cyanite_paths";

    private static Logger logger = Logger.getLogger(Dumper.class);

    private DistheneDumperParameters parameters;

    private TransportClient client;
    private Session session;

    public Dumper(DistheneDumperParameters parameters) {
        this.parameters = parameters;
    }

    public void dump() throws ExecutionException, InterruptedException, IOException {
        connectToES();
        connectToCassandra();

        // create directory
        File dayFolder = new File(parameters.getOutputLocation() + "/" + new DateTime(parameters.getStartTime() * 1000L, DateTimeZone.UTC).toString(DateTimeFormat.forPattern("yyyy-MM-dd")));
        if (!dayFolder.exists()) {
            //noinspection ResultOfMethodCallIgnored
            dayFolder.mkdir();
        }

        List<String> tenants = getTenants();

        for (String tenant : tenants) {
            if (!tenant.equals("tokyo")) {
                dumpTenant(dayFolder, tenant);
            }
        }

        session.close();
        session.getCluster().close();
        client.close();
    }

    private void dumpTenant(File folder, String tenant) throws IOException {
        logger.info("Dumping tenant: " + tenant);

        FileOutputStream fos = new FileOutputStream(folder.getAbsolutePath() + "/" + tenant + ".txt.gz");
        GZIPOutputStream gzos = new GZIPOutputStream(fos);
        final PrintWriter pwMetrics = new PrintWriter(gzos);

        logger.info("Getting paths");
        final List<String> paths = getTenantPaths(tenant);

        logger.info("Got " + paths.size() + " paths");

        PreparedStatement longRollupStatement = session.prepare(
                "select time, data from metric.metric where tenant = '" + tenant + "' and path = ? and rollup = 900 and period = 69120 and " +
                        "time >= " + parameters.getStartTime() + " and time <= " + parameters.getEndTime() +
                        " order by time asc"
        );

        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(parameters.getThreads()));
        final AtomicInteger counter = new AtomicInteger(0);

        for (String path : paths) {
            ListenableFuture<List<Metric>> future = executor.submit(new SinglePathCallable(session, longRollupStatement, path, tenant));
            Futures.addCallback(future, new FutureCallback<List<Metric>>() {
                @Override
                public void onSuccess(List<Metric> result) {
                    for (Metric metric : result) {
                        pwMetrics.println(metric);
                    }
                    int cc = counter.addAndGet(1);
                    if (cc % 10000 == 0) {
                        System.out.print("Processing: " + cc * 100 / paths.size() + "%\r");
                        pwMetrics.flush();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.error("Unexpected error:", t);
                }
            });
        }


        pwMetrics.flush();
        pwMetrics.close();
        gzos.close();
        fos.close();
        logger.info("Finished dumping tenant: " + tenant);
    }

    private List<String> getTenantPaths(String tenant) {
        final List<String> paths = new ArrayList<>();

        SearchResponse response = client.prepareSearch("cyanite_paths")
                .setScroll(new TimeValue(120000))
                .setSize(50000)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.filteredQuery(
                        QueryBuilders.regexpQuery("path", ".*"),
                        FilterBuilders.termFilter("tenant", tenant)), FilterBuilders.termFilter("leaf", true)))
                .addField("path")
                .execute().actionGet();

        while (response.getHits().getHits().length > 0) {
            for (SearchHit hit : response.getHits()) {
                paths.add(hit.field("path").<String>getValue());
            }

            response = client.prepareSearchScroll(response.getScrollId())
                    .setScroll(new TimeValue(120000))
                    .execute().actionGet();
        }


        return paths;
    }

    private List<String> getTenants() throws ExecutionException, InterruptedException {
        List<String> result = new ArrayList<>();

        SearchResponse response = client.prepareSearch(INDEX_NAME)
                .setSearchType(SearchType.COUNT)
                .addAggregation(AggregationBuilders.terms("agg").field("tenant")).execute().get();

        Collection<Terms.Bucket> buckets = ((Terms) response.getAggregations().get("agg")).getBuckets();

        for(Terms.Bucket bucket : buckets) {
            result.add(bucket.getKey());
        }

        return result;
    }

    private void connectToES() {
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", "cyanite").build();
        client = new TransportClient(settings);
        client.addTransportAddress(new InetSocketTransportAddress(parameters.getElasticSearchContactPoint(), 9300));
    }

    private void connectToCassandra() {
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setReceiveBufferSize(8388608);
        socketOptions.setSendBufferSize(1048576);
        socketOptions.setTcpNoDelay(false);
        socketOptions.setReadTimeoutMillis(1000000);
        socketOptions.setReadTimeoutMillis(1000000);

        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 8192);
        poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, 8192);
        poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.REMOTE, 128);
        poolingOptions.setMaxSimultaneousRequestsPerConnectionThreshold(HostDistance.LOCAL, 128);

        Cluster.Builder builder = Cluster.builder()
                .withSocketOptions(socketOptions)
                .withCompression(ProtocolOptions.Compression.LZ4)
                .withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()))
//                .withLoadBalancingPolicy(new WhiteListPolicy(new DCAwareRoundRobinPolicy(), Collections.singletonList(new InetSocketAddress(parameters.getCassandraContactPoint(), 9042))))
                .withPoolingOptions(poolingOptions)
                .withProtocolVersion(ProtocolVersion.V2)
                .withPort(9042);

        builder.addContactPoint(parameters.getCassandraContactPoint());

        Cluster cluster = builder.build();
        Metadata metadata = cluster.getMetadata();
        logger.debug("Connected to cluster: " + metadata.getClusterName());
        for (Host host : metadata.getAllHosts()) {
            logger.debug(String.format("Datacenter: %s; Host: %s; Rack: %s", host.getDatacenter(), host.getAddress(), host.getRack()));
        }

        session = cluster.connect();
    }

    private static class Metric {
        public String path;
        public Long time;
        public Double data;
        public String tenant;

        public Metric(String path, Long time, Double data, String tenant) {
            this.path = path;
            this.time = time;
            this.data = data;
            this.tenant = tenant;
        }

        @Override
        public String toString() {
            return path + " " + time + " " + data + " " + tenant;
        }
    }

    private static class SinglePathCallable implements Callable<List<Metric>> {
        private Session session;
        private PreparedStatement preparedStatement;
        private String path;
        private String tenant;

        public SinglePathCallable(Session session, PreparedStatement preparedStatement, String path, String tenant) {
            this.session = session;
            this.preparedStatement = preparedStatement;
            this.path = path;
            this.tenant = tenant;
        }

        @Override
        public List<Metric> call() throws Exception {
            List<Metric> metrics = new ArrayList<>();
            Statement statement = preparedStatement.bind(path);
            statement.setFetchSize(1000);
            ResultSet resultSet = session.execute(statement);

            for(Row row : resultSet) {
                metrics.add(new Metric(path, row.getLong("time"), ListUtils.average(row.getList("data", Double.class)), tenant));
            }

            return metrics;
        }
    }
}
