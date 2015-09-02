// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.mapreduce;

import org.kududb.client.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.junit.Assert.*;

public class TestInputFormatJob extends BaseKuduTest {
  private static final Logger LOG = LoggerFactory.getLogger(TestInputFormatJob.class);

  private static final String TABLE_NAME =
      TestInputFormatJob.class.getName() + "-" + System.currentTimeMillis();

  private static final HadoopTestingUtility HADOOP_UTIL = new HadoopTestingUtility();

  /** Counter enumeration to count the actual rows. */
  private static enum Counters { ROWS }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    BaseKuduTest.setUpBeforeClass();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    try {
      BaseKuduTest.tearDownAfterClass();
    } finally {
      HADOOP_UTIL.cleanup();
    }
  }

  @Test
  @SuppressWarnings("deprecation")
  public void test() throws Exception {

    KuduTable table = createFourTabletsTableWithNineRows(TABLE_NAME);

    Configuration conf = new Configuration();
    HADOOP_UTIL.setupAndGetTestDir(TestInputFormatJob.class.getName(), conf).getAbsolutePath();
    String jobName = TestInputFormatJob.class.getName();
    Job job = new Job(conf, jobName);

    Class<TestMapperTableInput> mapperClass = TestMapperTableInput.class;
    job.setJarByClass(mapperClass);
    job.setMapperClass(mapperClass);
    job.setNumReduceTasks(0);
    job.setOutputFormatClass(NullOutputFormat.class);
    new KuduTableMapReduceUtil.TableInputFormatConfigurator(
        job,
        TABLE_NAME,
        basicSchema.getColumnByIndex(0).getName(),
        getMasterAddresses())
        .operationTimeoutMs(DEFAULT_SLEEP)
        .addDependencies(false)
        .cacheBlocks(false)
        .configure();

    assertTrue("Test job did not end properly", job.waitForCompletion(true));

    assertEquals(9, job.getCounters().findCounter(Counters.ROWS).getValue());

    AsyncKuduScanner.AsyncKuduScannerBuilder builder = client.newScannerBuilder(table);
    assertEquals(9, countRowsInScan(builder.build()));
  }

  /**
   * Simple row counter and printer
   */
  static class TestMapperTableInput extends
      Mapper<NullWritable, RowResult, NullWritable, NullWritable> {

    @Override
    protected void map(NullWritable key, RowResult value, Context context) throws IOException,
        InterruptedException {
      context.getCounter(Counters.ROWS).increment(1);
      LOG.info(value.toStringLongFormat()); // useful to visual debugging
    }
  }

}
