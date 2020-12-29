package net.iponweb.disthene.dumper;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.HostFilterPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * @author Andrei Ivanov
 */
@SuppressWarnings("UnstableApiUsage")
public class Dumper {
    private static final String INDEX_NAME = "cyanite_paths";
    private static final String TABLE_QUERY = "SELECT COUNT(1) FROM SYSTEM.SCHEMA_COLUMNFAMILIES WHERE KEYSPACE_NAME=? AND COLUMNFAMILY_NAME=?";
    private static final String TENANT_TABLE_FORMAT = "metric_%s_%d";
    private static final String TENANT_KEYSPACE = "metric";

    private static final Logger logger = Logger.getLogger(Dumper.class);

    private final DistheneDumperParameters parameters;

    private TransportClient client;
    private Session session;

    private PreparedStatement tableQueryStatement;
    private boolean globalTableExists;

    Dumper(DistheneDumperParameters parameters) {
        this.parameters = parameters;
    }

    public void dump() throws ExecutionException, InterruptedException, IOException {
        connectToES();
        connectToCassandra();

        tableQueryStatement = session.prepare(TABLE_QUERY);

        // check global table
        globalTableExists = checkGlobalTable();

        logger.info("Global table exists");

        // create directory
        File dayFolder = new File(parameters.getOutputLocation() + "/" + new DateTime(parameters.getStartTime() * 1000L, DateTimeZone.UTC).toString(DateTimeFormat.forPattern("yyyy-MM-dd")));
        if (!dayFolder.exists()) {
            //noinspection ResultOfMethodCallIgnored
            dayFolder.mkdir();
        }

        List<String> tenants = getTenants();

        logger.info("Tenants:");
        for (String tenant : tenants) {
            logger.info("\t" + tenant);
        }
        for (String tenant : tenants) {
            dumpTenant(dayFolder, tenant);
        }

        session.close();
        session.getCluster().close();
        client.close();
    }

    private boolean checkGlobalTable() {
        ResultSet resultSet = session.execute(tableQueryStatement.bind("metric", "metric"));
        return resultSet.one().getLong(0) > 0;
    }

    private boolean checkTenantTable(String tenant) {
        ResultSet resultSet = session.execute(tableQueryStatement.bind(TENANT_KEYSPACE, String.format(TENANT_TABLE_FORMAT, tenant.replaceAll("[^0-9a-zA-Z_]", "_"), 900)));
        return resultSet.one().getLong(0) > 0;
    }

    private void dumpTenant(File folder, final String tenant) throws IOException {
        logger.info("Dumping tenant: " + tenant);

        FileOutputStream fos = new FileOutputStream(folder.getAbsolutePath() + "/" + tenant + ".txt.gz");
        GZIPOutputStream gzos = new GZIPOutputStream(fos);
        final PrintWriter pwMetrics = new PrintWriter(gzos);

        logger.info("Getting paths");
        final List<String> paths = getTenantPaths(tenant);

        logger.info("Got " + paths.size() + " paths");

        boolean tenantTableExists = checkTenantTable(tenant);
        logger.info(tenantTableExists ? "Tenant table exists" : "Tenant table doesn't exist");

        List<PreparedStatement> statements = new ArrayList<>();

        if (globalTableExists) {
            statements.add(
                    session.prepare(
                            "select time, data from metric.metric where tenant = '" + tenant + "' and path = ? and rollup = 900 and period = 69120 and " +
                                    "time >= " + parameters.getStartTime() + " and time <= " + parameters.getEndTime() +
                                    " order by time asc"
                    )
            );
        }

        if (tenantTableExists) {
            statements.add(
                    session.prepare(
                            String.format(
                                    "select time, data from " + TENANT_KEYSPACE + "." + TENANT_TABLE_FORMAT + " where path = ? and " +
                                            "time >= " + parameters.getStartTime() + " and time <= " + parameters.getEndTime() +
                                            " order by time asc",
                                    tenant.replaceAll("[^0-9a-zA-Z_]", "_"), 900
                            )
                    )
            );
        }

        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(parameters.getThreads()));
        final AtomicInteger counter = new AtomicInteger(0);

        for (String path : paths) {
            ListenableFuture<List<Metric>> future = executor.submit(new SinglePathCallable(session, statements, path, tenant));
            Futures.addCallback(future, new FutureCallback<List<Metric>>() {
                @Override
                public void onSuccess(List<Metric> result) {
                    for (Metric metric : result) {
                        pwMetrics.println(metric);
                    }

                    double cc = counter.addAndGet(1);

                    if (cc % 100000 == 0) {
                        logger.info("Processed: " + (int)((cc  / paths.size()) * 100) + "%");

                        pwMetrics.flush();
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.error("Unexpected error:", t);
                }
            });
        }

        executor.shutdown();
        try {
            //noinspection ResultOfMethodCallIgnored
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Failed: ", e);
        }

        pwMetrics.flush();
        pwMetrics.close();
        gzos.close();
        fos.close();
        logger.info("Finished dumping tenant: " + tenant);
    }

    private List<String> getTenantPaths(String tenant) {
        final List<String> paths = new ArrayList<>();

        SearchResponse response = client.prepareSearch(INDEX_NAME)
                .setScroll(new TimeValue(120000))
                .setSize(100000)
                .setQuery(QueryBuilders.filteredQuery(QueryBuilders.filteredQuery(
                        QueryBuilders.regexpQuery("path", ".*"),
                        FilterBuilders.termFilter("tenant", tenant)), FilterBuilders.termFilter("leaf", true)))
                .addField("path")
                .execute().actionGet();

        while (response.getHits().getHits().length > 0) {
            for (SearchHit hit : response.getHits()) {
                paths.add(hit.field("path").getValue());
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
                .addAggregation(AggregationBuilders.terms("agg").field("tenant").size(0))
                .setSize(10000)
                .execute().get();

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

    private void connectToCassandra() throws UnknownHostException {
        SocketOptions socketOptions = new SocketOptions();
        socketOptions.setReceiveBufferSize(8388608);
        socketOptions.setSendBufferSize(1048576);
        socketOptions.setTcpNoDelay(false);
        socketOptions.setReadTimeoutMillis(1000000);
        socketOptions.setReadTimeoutMillis(1000000);

        PoolingOptions poolingOptions = new PoolingOptions();
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, 32);
        poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, 32);
        poolingOptions.setMaxRequestsPerConnection(HostDistance.REMOTE, 128);
        poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL, 128);

        Cluster.Builder builder = Cluster.builder()
                .withSocketOptions(socketOptions)
                .withCompression(ProtocolOptions.Compression.LZ4)
                .withPoolingOptions(poolingOptions)
                .withProtocolVersion(ProtocolVersion.V2)
                .withPort(9042);

        if (parameters.getCassandraBlacklist().size() > 0) {
            final Set<String> blacklisted = new HashSet<>();
            for (String host : parameters.getCassandraBlacklist()) {
                blacklisted.add(InetAddress.getByName(host).getHostAddress());
            }
            builder.withLoadBalancingPolicy(new HostFilterPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()), host -> !blacklisted.contains(host.getAddress().getHostAddress())));

        } else {
            builder.withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()));
        }

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
        String path;
        Long time;
        Double data;
        String tenant;

        Metric(String path, Long time, Double data, String tenant) {
            this.path = path;
            this.time = time;
            this.data = data;
            this.tenant = tenant;
        }

        @Override
        public String toString() {
            return path + " " + data + " " + time + " " + tenant;
        }
    }

    private static class SinglePathCallable implements Callable<List<Metric>> {
        private final Session session;
        private final List<PreparedStatement> statements;
        private final String path;
        private final String tenant;

        SinglePathCallable(Session session, List<PreparedStatement> statements, String path, String tenant) {
            this.session = session;
            this.statements = statements;
            this.path = path;
            this.tenant = tenant;
        }

        @Override
        public List<Metric> call() {
            List<Metric> metrics = new ArrayList<>();

            for (PreparedStatement statement : statements) {
                ResultSet resultSet = session.execute(statement.bind(path));

                for(Row row : resultSet) {
                    metrics.add(new Metric(
                            path,
                            row.getLong("time"),
                            isSumMetric(path) ? ListUtils.sum(row.getList("data", Double.class)) : ListUtils.average(row.getList("data", Double.class)),
                            tenant
                    ));
                }

            }
            return metrics;
        }

        private static boolean isSumMetric(String path) {
            return path.startsWith("sum");

        }
    }
}
