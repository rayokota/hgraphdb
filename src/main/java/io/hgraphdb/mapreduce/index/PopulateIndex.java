package io.hgraphdb.mapreduce.index;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import io.hgraphdb.*;
import io.hgraphdb.mapreduce.TableInputFormat;
import org.apache.commons.cli.*;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * An MR job to populate the index.
 *
 * Based on IndexTool from Phoenix.
 */
public class PopulateIndex extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(PopulateIndex.class);

    private static final Option INDEX_TYPE_OPTION = new Option("t", "type", true,
            "Index type (edge or vertex)");
    private static final Option LABEL_OPTION = new Option("l", "label", true,
            "Label to be indexed");
    private static final Option PROPERTY_KEY_OPTION = new Option("p", "property-key", true,
            "Property key to be indexed");
    private static final Option DIRECT_API_OPTION = new Option("d", "direct", false,
            "If specified, we avoid the bulk load (optional)");
    private static final Option RUN_FOREGROUND_OPTION = new Option("rf", "run-foreground", false,
            "Whether to populate index in foreground");
    private static final Option OUTPUT_PATH_OPTION = new Option("op", "output-path", true,
            "Output path where the files are written");
    private static final Option SKIP_DEPENDENCY_JARS = new Option("sj", "skip-dependency-jars", false,
            "Skip adding dependency jars");
    private static final Option CUSTOM_ARGS_OPTION = new Option("ca", "customArguments", true,
            "Provide custom" +
            " arguments for the job configuration in the form:" +
            " -ca <param1>=<value1>,<param2>=<value2> -ca <param3>=<value3> etc." +
            " It can appear multiple times, and the last one has effect" +
            " for the same param.");
    private static final Option HELP_OPTION = new Option("h", "help", false, "Help");

    private Options getOptions() {
        final Options options = new Options();
        options.addOption(INDEX_TYPE_OPTION);
        options.addOption(LABEL_OPTION);
        options.addOption(PROPERTY_KEY_OPTION);
        options.addOption(DIRECT_API_OPTION);
        options.addOption(RUN_FOREGROUND_OPTION);
        options.addOption(OUTPUT_PATH_OPTION);
        options.addOption(SKIP_DEPENDENCY_JARS);
        options.addOption(CUSTOM_ARGS_OPTION);
        options.addOption(HELP_OPTION);
        return options;
    }

    /**
     * Parses the commandline arguments, throws IllegalStateException if mandatory arguments are
     * missing.
     * @param args supplied command line arguments
     * @return the parsed command line
     */
    private CommandLine parseOptions(String[] args) {

        final Options options = getOptions();

        CommandLineParser parser = new PosixParser();
        CommandLine cmdLine = null;
        try {
            cmdLine = parser.parse(options, args);
        } catch (ParseException e) {
            printHelpAndExit("Error parsing command line options: " + e.getMessage(), options);
        }

        if (cmdLine.hasOption(HELP_OPTION.getOpt())) {
            printHelpAndExit(options, 0);
        }

        if (!cmdLine.hasOption(INDEX_TYPE_OPTION.getOpt())) {
            throw new IllegalStateException(INDEX_TYPE_OPTION.getLongOpt() + " is a mandatory "
                    + "parameter");
        }

        if (!cmdLine.hasOption(LABEL_OPTION.getOpt())) {
            throw new IllegalStateException(LABEL_OPTION.getLongOpt() + " is a mandatory "
                    + "parameter");
        }

        if (!cmdLine.hasOption(PROPERTY_KEY_OPTION.getOpt())) {
            throw new IllegalStateException(PROPERTY_KEY_OPTION.getLongOpt() + " is a mandatory "
                    + "parameter");
        }

        if (!cmdLine.hasOption(OUTPUT_PATH_OPTION.getOpt())) {
            throw new IllegalStateException(OUTPUT_PATH_OPTION.getLongOpt() + " is a mandatory "
                    + "parameter");
        }

        if (!cmdLine.hasOption(DIRECT_API_OPTION.getOpt())
                && cmdLine.hasOption(RUN_FOREGROUND_OPTION.getOpt())) {
            throw new IllegalStateException(RUN_FOREGROUND_OPTION.getLongOpt()
                    + " is applicable only for " + DIRECT_API_OPTION.getLongOpt());
        }
        return cmdLine;
    }

    private void printHelpAndExit(String errorMessage, Options options) {
        System.err.println(errorMessage);
        printHelpAndExit(options, 1);
    }

    private void printHelpAndExit(Options options, int exitCode) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("help", options);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        HBaseGraph graph = null;
        try {
            CommandLine cmdLine = null;
            try {
                cmdLine = parseOptions(args);
            } catch (IllegalStateException e) {
                printHelpAndExit(e.getMessage(), getOptions());
            }
            final Configuration configuration = HBaseConfiguration.addHbaseResources(getConf());

            final String type = cmdLine.getOptionValue(INDEX_TYPE_OPTION.getOpt());
            ElementType indexType;
            try {
                indexType = ElementType.valueOf(type.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new IllegalStateException(INDEX_TYPE_OPTION.getLongOpt() + " must be one of 'edge' or 'vertex'");
            }
            final String label = cmdLine.getOptionValue(LABEL_OPTION.getOpt());
            final String propertyKey = cmdLine.getOptionValue(PROPERTY_KEY_OPTION.getOpt());

            configuration.set(Constants.POPULATE_INDEX_TYPE, indexType.toString());
            configuration.set(Constants.POPULATE_INDEX_LABEL, label);
            configuration.set(Constants.POPULATE_INDEX_PROPERTY_KEY, propertyKey);

            boolean skipDependencyJars = cmdLine.hasOption(SKIP_DEPENDENCY_JARS.getOpt());

            if (cmdLine.hasOption(CUSTOM_ARGS_OPTION.getOpt())) {
                for (String caOptionValue : cmdLine.getOptionValues(CUSTOM_ARGS_OPTION.getOpt())) {
                    for (String paramValue :
                            Splitter.on(',').split(caOptionValue)) {
                        String[] parts = Iterables.toArray(Splitter.on('=').split(paramValue),
                                String.class);
                        if (parts.length != 2) {
                            throw new IllegalArgumentException("Unable to parse custom " +
                                    " argument: " + paramValue);
                        }
                        if (LOG.isInfoEnabled()) {
                            LOG.info("Setting custom argument [" + parts[0] + "] to [" +
                                    parts[1] + "] in Configuration");
                        }
                        configuration.set(parts[0], parts[1]);
                    }
                }
            }

            HBaseGraphConfiguration hconf = new HBaseGraphConfiguration(configuration);
            graph = new HBaseGraph(hconf);
            if (!graph.hasIndex(OperationType.WRITE, indexType, label, propertyKey)) {
                throw new IllegalArgumentException(String.format(
                    "No %s index found for label '%s' and property key '%s'", indexType, label, propertyKey));
            }
            TableName inputTableName = HBaseGraphUtils.getTableName(
                    hconf, indexType == ElementType.EDGE ? Constants.EDGES : Constants.VERTICES);
            TableName outputTableName = HBaseGraphUtils.getTableName(
                    hconf, indexType == ElementType.EDGE ? Constants.EDGE_INDICES : Constants.VERTEX_INDICES);

            String jobName = indexType.toString().toLowerCase() + "_" + label + "_" + propertyKey;
            final Path outputPath = new Path(cmdLine.getOptionValue(OUTPUT_PATH_OPTION.getOpt()), jobName);
            FileSystem.get(configuration).delete(outputPath, true);
            
            final Job job = Job.getInstance(configuration, jobName);
            job.setJarByClass(PopulateIndex.class);
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            FileOutputFormat.setOutputPath(job, outputPath);

            job.setInputFormatClass(TableInputFormat.class);
            job.getConfiguration().set(TableInputFormat.INPUT_TABLE, inputTableName.getNameAsString());
            job.getConfiguration().set(TableInputFormat.SCAN, convertScanToString(new Scan()));

            TableMapReduceUtil.initCredentials(job);
            
            boolean useDirectApi = cmdLine.hasOption(DIRECT_API_OPTION.getOpt());
            if (useDirectApi) {
                configureSubmittableJobUsingDirectApi(job, outputPath, outputTableName, skipDependencyJars,
                    cmdLine.hasOption(RUN_FOREGROUND_OPTION.getOpt()));
            } else {
                configureRunnableJobUsingBulkLoad(job, outputPath, outputTableName, skipDependencyJars);
                // Without direct API, we need to update the index state to ACTIVE from client.
                graph.updateIndex(new IndexMetadata.Key(indexType, label, propertyKey), IndexMetadata.State.ACTIVE);
            }
            return 0;
        } catch (Exception ex) {
            LOG.error("An exception occurred while performing the indexing job: "
                    + ExceptionUtils.getMessage(ex) + " at:\n" + ExceptionUtils.getStackTrace(ex));
            return -1;
        } finally {
            if (graph != null) {
                graph.close();
            }
        }
    }

    /**
     * Submits the job and waits for completion.
     * @param job job
     * @param outputPath output path
     * @throws Exception
     */
    private void configureRunnableJobUsingBulkLoad(Job job, Path outputPath, TableName outputTableName,
                                                   boolean skipDependencyJars) throws Exception {
             job.setMapperClass(HBaseIndexImportMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        final Configuration configuration = job.getConfiguration();
        try (Connection conn = ConnectionFactory.createConnection(configuration);
             Admin admin = conn.getAdmin();
             Table table = conn.getTable(outputTableName);
             RegionLocator regionLocator = conn.getRegionLocator(outputTableName)) {
            HFileOutputFormat2.configureIncrementalLoad(job, table, regionLocator);
            if (skipDependencyJars) {
                job.getConfiguration().unset("tmpjars");
            }
            boolean status = job.waitForCompletion(true);
            if (!status) {
                LOG.error("IndexTool job failed!");
                throw new Exception("IndexTool job failed: " + job.toString());
            }

            LOG.info("Loading HFiles from {}", outputPath);
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(configuration);
            loader.doBulkLoad(outputPath, admin, table, regionLocator);
        }
        FileSystem.get(configuration).delete(outputPath, true);
    }
    
    /**
     * Uses the HBase Front Door Api to write to index table. Submits the job and either returns or
     * waits for the job completion based on runForeground parameter.
     * 
     * @param job job
     * @param outputPath output path
     * @param runForeground - if true, waits for job completion, else submits and returns
     *            immediately.
     * @throws Exception
     */
    private void configureSubmittableJobUsingDirectApi(Job job, Path outputPath, TableName outputTableName,
                                                       boolean skipDependencyJars, boolean runForeground)
            throws Exception {
        job.setMapperClass(HBaseIndexImportDirectMapper.class);
        job.setReducerClass(HBaseIndexImportDirectReducer.class);
        Configuration conf = job.getConfiguration();
        HBaseConfiguration.merge(conf, HBaseConfiguration.create(conf));
        conf.set(TableOutputFormat.OUTPUT_TABLE, outputTableName.getNameAsString());

        //Set the Output classes
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        if (!skipDependencyJars) {
            TableMapReduceUtil.addDependencyJars(job);
        }
        job.setNumReduceTasks(1);

        if (!runForeground) {
            LOG.info("Running Index Build in Background - Submit async and exit");
            job.submit();
            return;
        }
        LOG.info("Running Index Build in Foreground. Waits for the build to complete. This may take a long time!.");
        boolean result = job.waitForCompletion(true);
        if (!result) {
            LOG.error("IndexTool job failed!");
            throw new Exception("IndexTool job failed: " + job.toString());
        }
        FileSystem.get(conf).delete(outputPath, true);
    }

    /**
     * Writes the given scan into a Base64 encoded string.
     *
     * @param scan  The scan to write out.
     * @return The scan saved in a Base64 encoded string.
     * @throws IOException When writing the scan fails.
     */
    static String convertScanToString(Scan scan) throws IOException {
        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);
        return Base64.encodeBytes(proto.toByteArray());
    }

    public static void main(final String[] args) throws Exception {
        int result = ToolRunner.run(new PopulateIndex(), args);
        System.exit(result);
    }

}
