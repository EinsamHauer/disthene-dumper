package net.iponweb.disthene.dumper;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.loadbalancing.DcInferringLoadBalancingPolicy;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;

/**
 * @author Andrei Ivanov
 */
public class Dumper {
    private static final String INDEX_NAME = "disthene";
    private static final String TABLE_FORMAT = "metric_%s_%d";
    private static final String KEYSPACE = "metric";

    private static final Logger logger = LogManager.getLogger(Dumper.class);

    private final DistheneDumperParameters parameters;

    private RestHighLevelClient client;
    private CqlSession session;

    Dumper(DistheneDumperParameters parameters) {
        this.parameters = parameters;
    }

    public void dump() throws InterruptedException, IOException {
        connectToES();
        connectToCassandra();

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
        client.close();
    }

    private void dumpTenant(File folder, final String tenant) throws IOException, InterruptedException {
        logger.info("Dumping tenant: " + tenant);

        FileOutputStream fos = new FileOutputStream(folder.getAbsolutePath() + "/" + tenant + ".txt.gz");
        GZIPOutputStream gzos = new GZIPOutputStream(fos);
        final PrintWriter pwMetrics = new PrintWriter(gzos);

        PreparedStatement statement = session.prepare(
                String.format(
                        "select time, data from " + KEYSPACE + "." + TABLE_FORMAT + " where path = ? and " +
                                "time >= " + parameters.getStartTime() + " and time <= " + parameters.getEndTime() +
                                " order by time asc",
                        tenant.replaceAll("[^0-9a-zA-Z_]", "_"), 900
                ));

        ExecutorService executor = Executors.newFixedThreadPool(parameters.getThreads());
        final AtomicInteger counter = new AtomicInteger(0);

        CountRequest countRequest = new CountRequest(INDEX_NAME)
                .query(QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("tenant", tenant))
                        .must(QueryBuilders.termQuery("leaf", true)));

        CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);

        long total = countResponse.getCount();
        logger.info("Got " + total + " paths");

        final Scroll scroll = new Scroll(TimeValue.timeValueHours(4L));

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .fetchSource("path", null)
                .query(
                        QueryBuilders.boolQuery()
                                .must(QueryBuilders.termQuery("tenant", tenant))
                                .must(QueryBuilders.termQuery("leaf", true))
                );

        SearchRequest request = new SearchRequest(INDEX_NAME)
                .source(sourceBuilder)
                .scroll(scroll);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        String scrollId = response.getScrollId();

        Semaphore semaphore = new Semaphore(parameters.getThreads() * 2);

        SearchHits hits = response.getHits();

        while (hits.getHits().length > 0) {
            for (SearchHit hit : hits) {
                String path = String.valueOf(hit.getSourceAsMap().get("path"));

                semaphore.acquire();

                CompletableFuture.supplyAsync(new SinglePathSupplier(session, statement, path, tenant), executor)
                        .whenComplete((metrics, throwable) -> {
                            if (throwable != null) {
                                logger.error("Cassandra request failed", throwable);
                            } else {
                                for (Metric metric : metrics) {
                                    pwMetrics.println(metric);
                                }

                                double cc = counter.addAndGet(1);

                                if (cc % 100000 == 0) {
                                    logger.info("Processed: " + (int) ((cc / total) * 100) + "%");

                                    pwMetrics.flush();
                                }
                            }

                            semaphore.release();
                        });
            }

            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId).scroll(scroll);
            response = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = response.getScrollId();
            hits = response.getHits();
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

    private List<String> getTenants() throws IOException {
        List<String> result = new ArrayList<>();

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
                .query(QueryBuilders.matchAllQuery())
                .size(10000)
                .aggregation(AggregationBuilders.terms("agg").field("tenant.keyword").size(10000));

        SearchRequest request = new SearchRequest(INDEX_NAME)
                .source(sourceBuilder);

        SearchResponse response = client.search(request, RequestOptions.DEFAULT);

        Collection<? extends Terms.Bucket> buckets = ((Terms) response.getAggregations().get("agg")).getBuckets();

        for (Terms.Bucket bucket : buckets) {
            result.add(bucket.getKeyAsString());
        }


        return result;
    }

    private void connectToES() {
        client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(parameters.getElasticSearchContactPoint(), 9200)));
    }

    private void connectToCassandra() {
        DriverConfigLoader loader =
                DriverConfigLoader.programmaticBuilder()
                        .withString(DefaultDriverOption.PROTOCOL_COMPRESSION, "lz4")
                        .withStringList(DefaultDriverOption.CONTACT_POINTS, List.of(parameters.getCassandraContactPoint() + ":9042"))
                        .withInt(DefaultDriverOption.CONNECTION_MAX_REQUESTS, 128)
                        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofMillis(1_000_000))
                        .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "ONE")
                        .withClass(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS, DcInferringLoadBalancingPolicy.class)
                        .build();

        session = CqlSession.builder().withConfigLoader(loader).build();

        Metadata metadata = session.getMetadata();
        logger.debug("Connected to cluster: " + metadata.getClusterName());
        for (Node node : metadata.getNodes().values()) {
            logger.debug(String.format("Datacenter: %s; Host: %s; Rack: %s",
                    node.getDatacenter(),
                    node.getBroadcastAddress().isPresent() ? node.getBroadcastAddress().get().toString() : "unknown", node.getRack()));
        }
    }

    private static class Metric {
        final String path;
        final Long time;
        final Double data;
        final String tenant;

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

    private static class SinglePathSupplier implements Supplier<List<Metric>> {
        private final CqlSession session;
        private final PreparedStatement statement;
        private final String path;
        private final String tenant;

        public SinglePathSupplier(CqlSession session, PreparedStatement statement, String path, String tenant) {
            this.session = session;
            this.statement = statement;
            this.path = path;
            this.tenant = tenant;
        }

        @Override
        public List<Metric> get() {
            List<Metric> metrics = new ArrayList<>();

            ResultSet resultSet = session.execute(statement.bind(path));

            for (Row row : resultSet) {
                metrics.add(new Metric(
                        path,
                        row.getLong("time"),
                        isSumMetric(path) ? ListUtils.sum(Objects.requireNonNull(row.getList("data", Double.class))) : ListUtils.average(Objects.requireNonNull(row.getList("data", Double.class))),
                        tenant
                ));
            }

            return metrics;
        }

        private static boolean isSumMetric(String path) {
            return path.startsWith("sum");

        }
    }
}
