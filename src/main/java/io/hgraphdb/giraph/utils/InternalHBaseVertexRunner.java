package io.hgraphdb.giraph.utils;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import io.hgraphdb.Constants;
import io.hgraphdb.HBaseGraphConfiguration;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.job.GiraphJob;
import org.apache.giraph.utils.FileUtils;
import org.apache.giraph.zk.InProcessZooKeeperRunner;
import org.apache.giraph.zk.ZookeeperConfig;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;

//import org.apache.giraph.io.formats.FileOutputFormatUtil;

/**
 * A base class for running internal tests on a vertex
 * <p>
 * Extending classes only have to invoke the run() method to test their vertex.
 * All data is written to a local tmp directory that is removed afterwards.
 * A local zookeeper instance is started in an extra thread and
 * shutdown at the end.
 * <p>
 * Heavily inspired from Apache Mahout's MahoutTestCase
 */
@SuppressWarnings("unchecked")
public class InternalHBaseVertexRunner {

    /**
     * Logger
     */
    private static final Logger LOG =
            Logger.getLogger(org.apache.giraph.utils.InternalVertexRunner.class);

    /**
     * Don't construct
     */
    private InternalHBaseVertexRunner() {
    }

    /**
     * Run the ZooKeeper in-process and the job.
     *
     * @param zookeeperConfig Quorum peer configuration
     * @param giraphJob       Giraph job to run
     * @return True if successful, false otherwise
     */
    private static boolean runZooKeeperAndJob(
            final ZookeeperConfig zookeeperConfig,
            GiraphJob giraphJob) throws IOException {
        final InProcessZooKeeperRunner.ZooKeeperServerRunner zookeeper =
                new InProcessZooKeeperRunner.ZooKeeperServerRunner();

        int port = zookeeper.start(zookeeperConfig);

        LOG.info("Started test zookeeper on port " + port);
        GiraphConstants.ZOOKEEPER_LIST.set(giraphJob.getConfiguration(),
                "localhost:" + port);
        try {
            return giraphJob.run(true);
        } catch (InterruptedException |
                ClassNotFoundException | IOException e) {
            LOG.error("runZooKeeperAndJob: Got exception on running", e);
        } finally {
            zookeeper.stop();
        }

        return false;
    }

    /**
     * Attempts to run the vertex internally in the current JVM, reading from and
     * writing to a temporary folder on local disk. Will start its own zookeeper
     * instance.
     *
     * @param conf GiraphClasses specifying which types to use
     * @return linewise output data, or null if job fails
     * @throws Exception if anything goes wrong
     */
    public static Iterable<String> run(
            GiraphConfiguration conf) throws Exception {
        // Prepare input file, output folder and temporary folders
        File tmpDir = FileUtils.createTestDir(conf.getComputationName());
        try {
            return run(conf, null, tmpDir);
        } finally {
            FileUtils.delete(tmpDir);
        }
    }

    /**
     * Attempts to run the vertex internally in the current JVM, reading from and
     * writing to a temporary folder on local disk. Will start its own zookeeper
     * instance.
     *
     * @param conf           GiraphClasses specifying which types to use
     * @param checkpointsDir if set, will use this folder
     *                       for storing checkpoints.
     * @param tmpDir         file path for storing temporary files.
     * @return linewise output data, or null if job fails
     * @throws Exception if anything goes wrong
     */
    public static Iterable<String> run(
            GiraphConfiguration conf,
            String checkpointsDir,
            File tmpDir) throws Exception {

        String ns = conf.get(HBaseGraphConfiguration.Keys.GRAPH_NAMESPACE);
        String prefix = conf.get(HBaseGraphConfiguration.Keys.GRAPH_TABLE_PREFIX);
        String tablePrefix = (ns != null ? ns + TableName.NAMESPACE_DELIM : "") + (prefix != null ? prefix : "");
        conf.set(Constants.EDGE_INPUT_TABLE, tablePrefix + Constants.EDGES);
        conf.set(Constants.VERTEX_INPUT_TABLE, tablePrefix + Constants.VERTICES);

        File outputDir = FileUtils.createTempDir(tmpDir, "output");
        File zkDir = FileUtils.createTempDir(tmpDir, "_bspZooKeeper");
        File zkMgrDir = FileUtils.createTempDir(tmpDir, "_defaultZkManagerDir");

        conf.setWorkerConfiguration(1, 1, 100.0f);
        GiraphConstants.SPLIT_MASTER_WORKER.set(conf, false);
        GiraphConstants.LOCAL_TEST_MODE.set(conf, true);

        conf.set(GiraphConstants.ZOOKEEPER_DIR, zkDir.toString());
        GiraphConstants.ZOOKEEPER_MANAGER_DIRECTORY.set(conf,
                zkMgrDir.toString());

        if (checkpointsDir == null) {
            checkpointsDir = FileUtils.createTempDir(
                    tmpDir, "_checkpoints").toString();
        }
        GiraphConstants.CHECKPOINT_DIRECTORY.set(conf, checkpointsDir);

        // Create and configure the job to run the vertex
        GiraphJob job = new GiraphJob(conf, conf.getComputationName());

        Job internalJob = job.getInternalJob();
        //FileOutputFormatUtil.setOutputPath(job.getInternalJob(),
        //        new Path(outputDir.toString()));
        internalJob.getConfiguration().set("mapred.output.dir", outputDir.toString());

        // Configure a local zookeeper instance
        ZookeeperConfig qpConfig = configLocalZooKeeper(zkDir);

        boolean success = runZooKeeperAndJob(qpConfig, job);
        if (!success) {
            return null;
        }

        File outFile = new File(outputDir, "part-m-00000");
        if (conf.hasVertexOutputFormat() && outFile.canRead()) {
            return Files.readLines(outFile, Charsets.UTF_8);
        } else {
            return ImmutableList.of();
        }

    }

    /**
     * Configuration options for running local ZK.
     *
     * @param zkDir directory for ZK to hold files in.
     * @return zookeeper configuration object
     */
    private static ZookeeperConfig configLocalZooKeeper(File zkDir) {
        ZookeeperConfig config = new ZookeeperConfig();
        config.setMaxSessionTimeout(100000);
        config.setMinSessionTimeout(10000);
        config.setClientPortAddress(new InetSocketAddress("localhost", 0));
        config.setDataDir(zkDir.getAbsolutePath());
        return config;
    }

}
