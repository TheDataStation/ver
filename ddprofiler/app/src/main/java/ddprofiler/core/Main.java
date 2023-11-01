package ddprofiler.core;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import joptsimple.OptionParser;
import metrics.Metrics;

import ddprofiler.core.config.CommandLineArgs;
import ddprofiler.core.config.ConfigKey;
import ddprofiler.core.config.ProfilerConfig;
import ddprofiler.sources.Source;
import ddprofiler.sources.SourceType;
import ddprofiler.sources.YAMLParser;
import ddprofiler.sources.config.SourceConfig;
import ddprofiler.store.Store;
import ddprofiler.store.StoreFactory;

public class Main {

    final private Logger LOG = LoggerFactory.getLogger(Main.class.getName());

    public enum ExecutionMode {
        ONLINE(0), OFFLINE_FILES(1), OFFLINE_DB(2), BENCHMARK(3);
        int mode;

        ExecutionMode(int mode) {
            this.mode = mode;
        }
    }

    public void startProfiler(ProfilerConfig pc, OptionParser parser) throws IOException {

        long start = System.nanoTime();

        Store s = null;
        try {
            s = StoreFactory.makeStoreOfType(pc.getInt(ProfilerConfig.STORE_TYPE), pc);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Conductor c = new Conductor(pc, s);
        c.start();

        // TODO: Configure mode here ?
        // int executionMode = pc.getInt(ProfilerConfig.EXECUTION_MODE);
        // if (executionMode == ExecutionMode.ONLINE.mode) {
        // // Start infrastructure for REST server
        // WebServer ws = new WebServer(pc, c);
        // ws.init();
        // }

        // Parsing profile schema file
        try {
            String profileSchemaFile = pc.getString(ProfilerConfig.PROFILE_SCHEMA_FILE);
            LOG.info("Using {} as profile schema file", profileSchemaFile);
            ProfileSchemaParser.processProfileSchema(profileSchemaFile);
        } catch (FileNotFoundException fnfe) {
            LOG.error("Profile schema file not found");
            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Parsing sources config file
        String sourceConfigFile = pc.getString(ProfilerConfig.SOURCE_CONFIG_FILE);
        LOG.info("Using {} as sources file", sourceConfigFile);

        List<SourceConfig> sourceConfigs = null;
        try {
            sourceConfigs = YAMLParser.processSourceConfig(sourceConfigFile);
        } catch (FileNotFoundException fnfe) {
            LOG.error("Need to indicate valid sourceConfigFile via --sources; see help");
            parser.printHelpOn(System.out);
            System.exit(0);
        }
        catch (Exception e) {
            e.printStackTrace();
        }

        List<Source> allSources = new ArrayList<>();
        LOG.info("Found {} sources to profile", sourceConfigs.size());
        for (SourceConfig sourceConfig : sourceConfigs) {
            String sourceName = sourceConfig.getSourceName();
            SourceType sType = sourceConfig.getSourceType();
            LOG.info("Processing source {} of type {}", sourceName, sType);
            Source source = SourceType.instantiateSourceOfType(sType);
            List<Source> sources = source.processSource(sourceConfig);
            allSources.addAll(sources);
        }
        for (Source source : allSources) {
            c.submitTask(source);
        }

        while (c.isTherePendingWork()) {
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        c.stop();
        s.tearDownStore();

        long end = System.nanoTime();
        LOG.info("Finished processing in {}", (end - start));
    }

    public static void main(String args[]) {

        // Get Properties with command line configuration
        List<ConfigKey> configKeys = ProfilerConfig.getAllConfigKey();
        OptionParser parser = new OptionParser();
        // Unrecognized options are passed through to the query
        parser.allowsUnrecognizedOptions();
        CommandLineArgs cla = new CommandLineArgs(args, parser, configKeys);
        Properties commandLineProperties = cla.getProperties();

        // Check if the user requests help
        for (String a : args) {
            if (a.contains("help") || a.equals("?")) {
                try {
                    parser.printHelpOn(System.out);
                    System.exit(0);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        Properties validatedProperties = validateProperties(commandLineProperties);

        ProfilerConfig pc = new ProfilerConfig(validatedProperties);

        // Start main
        configureMetricsReporting(pc);

        // config logs
        configLog();

        Main m = new Main();
        try {
            m.startProfiler(pc, parser);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    private static void configLog() {
//		final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger("com.zaxxer.hikari");
//		if (!(logger instanceof ch.qos.logback.classic.Logger)) {
//			return;
//		}
//		ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) logger;
//		logbackLogger.setLevel(ch.qos.logback.classic.Level.WARN);
    }

    static private void configureMetricsReporting(ProfilerConfig pc) {
        int reportConsole = pc.getInt(ProfilerConfig.REPORT_METRICS_CONSOLE);
        if (reportConsole > 0) {
            Metrics.startConsoleReporter(reportConsole);
        }
    }

    public static Properties validateProperties(Properties p) {
        // TODO: Go over all properties configured here and validate their
        // ranges,
        // values
        // etc. Stop the program and spit useful doc message when something goes
        // wrong.
        // Return the unmodified properties if everything goes well.

        return p;
    }
}
