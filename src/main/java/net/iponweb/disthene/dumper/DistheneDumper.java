package net.iponweb.disthene.dumper;

import org.apache.commons.cli.*;
import org.apache.log4j.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.File;
import java.io.IOException;

/**
 * @author Andrei Ivanov
 */
public class DistheneDumper {

    private static Logger logger;

    private static final String DEFAULT_ROLLUP_STRING = "900s:62208000s";
    private static final int DEFAULT_THREADS = 8;

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("l", "log-location", true, "log file location");
        options.addOption("ll", "log-level", true, "log level (i.e.: DEBUG, INFO, ERROR, etc)");
        options.addOption("r", "rollups", true, "rollups to dump; comma separated, format like 900s:62208000s");
        options.addOption("o", "output-path", true, "output path");
        options.addOption("d", "date", true, "date to dump, format like 20150101");
        options.addOption("c", "cassandra", true, "Cassandra contact point");
        options.addOption("e", "elasticsearch", true, "Elasticsearch contact point");
        options.addOption("t", "threads", true, "number of threads");

        CommandLineParser parser = new GnuParser();

        try {
            CommandLine commandLine = parser.parse(options, args);

            String logLocation = commandLine.hasOption("l") ? commandLine.getOptionValue("l") : null;
            String logLevel = commandLine.hasOption("ll") ? commandLine.getOptionValue("ll") : "INFO";
            configureLog(logLocation, logLevel);

            DistheneDumperParameters parameters = new DistheneDumperParameters();
            if (!commandLine.hasOption("o")) {
                logger.error("No output path specified");
                System.exit(2);
            }

            String outputPath = commandLine.getOptionValue("o");
            File f = new File(outputPath);
            if (!f.exists() || !f.isDirectory()) {
                logger.error("Output path is not valid");
                System.exit(3);
            }

            parameters.setOutputLocation(outputPath);

            DateTime dateToDump = DateTime.now(DateTimeZone.UTC).minusDays(1).withMillisOfDay(0);
            if (commandLine.hasOption("d")) {
                DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMdd");
                try {
                    dateToDump = formatter.parseDateTime(commandLine.getOptionValue("d"));
                } catch (Exception e) {
                    logger.error("Invalid date specified");
                    System.exit(4);
                }
            }

            parameters.setStartTime(dateToDump.getMillis() / 1000L);
            parameters.setEndTime(dateToDump.plusDays(1).minusMillis(1).getMillis() / 1000L);

            String rollupsString = commandLine.hasOption("r") ? commandLine.getOptionValue("r") : DEFAULT_ROLLUP_STRING;
            String[] split = rollupsString.split(",");

            for (String rollupString : split) {
                parameters.addRollup(rollupString);
            }

            if (!commandLine.hasOption("c")) {
                logger.error("Cassandra contact point is not specified");
                System.exit(5);
            }

            if (!commandLine.hasOption("e")) {
                logger.error("Elasticsearch contact point is not specified");
                System.exit(6);
            }

            parameters.setThreads(DEFAULT_THREADS);
            if (commandLine.hasOption("t")) {
                try {
                    parameters.setThreads(Integer.parseInt(commandLine.getOptionValue("t")));
                } catch (Exception ignored) {

                }
            }

            parameters.setCassandraContactPoint(commandLine.getOptionValue("c"));
            parameters.setElasticSearchContactPoint(commandLine.getOptionValue("e"));

            logger.info("Running with the following parameters: " + parameters);
            new Dumper(parameters).dump();

            logger.info("All done");
            System.exit(0);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("Disthene-dumper", options);
            System.exit(1);
        } catch (Exception e) {
            logger.error("Unexpected error: ", e);
            System.exit(100);
        }

    }

    private static void configureLog(String location, String level) {
        Level logLevel = Level.toLevel(level, Level.INFO);

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(logLevel);

        PatternLayout layout = new PatternLayout("%p %d{dd.MM.yyyy HH:mm:ss,SSS} [%t] %c %x - %m%n");
        rootLogger.addAppender(new ConsoleAppender(layout));

        logger = Logger.getLogger(DistheneDumper.class);

        if (location != null) {
            try {
                RollingFileAppender fileAppender = new RollingFileAppender(layout, location);
                rootLogger.addAppender(fileAppender);
            } catch (IOException e) {
                logger.error("Failed to add file appender: ", e);
            }
        }


    }


}
