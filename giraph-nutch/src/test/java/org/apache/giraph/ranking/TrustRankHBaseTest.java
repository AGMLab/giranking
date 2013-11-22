/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.giraph.ranking;


import org.apache.giraph.BspCase;
import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.job.GiraphJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.log4j.Logger;
import org.apache.giraph.ranking.LinkRank.TrustRankComputation;
import org.apache.giraph.ranking.LinkRank.TrustRankVertexMasterCompute;
import org.apache.giraph.ranking.LinkRank.io.HostRankVertexFilter;
import org.apache.giraph.ranking.LinkRank.io.Nutch2HostOutputFormat;
import org.apache.giraph.ranking.LinkRank.io.Nutch2HostTrustInputFormat;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import static org.junit.Assert.*;

/**
 * Tests for {@link org.apache.giraph.ranking.LinkRank.LinkRankComputation}
 * Test case for LinkRank reading edges and vertex scores
 * from HBase, calculates new scores and updates the HBase
 * Table again.
 */
public class TrustRankHBaseTest extends BspCase {


  private final Logger LOG = Logger.getLogger(org.apache.giraph.ranking.TrustRankHBaseTest.class);

  private static final String TABLE_NAME = "simple_graph";
  private static final double DELTA = 1e-3;

  private HBaseTestingUtility testUtil;
  private Path hbaseRootdir;


  public TrustRankHBaseTest() {
    super(org.apache.giraph.ranking.HostRankHBaseTest.class.getName());

    // Let's set up the hbase root directory.
    Configuration conf = HBaseConfiguration.create();
    try {
      FileSystem fs = FileSystem.get(conf);
      String randomStr = UUID.randomUUID().toString();
      String tmpdir = System.getProperty("java.io.tmpdir") + "/" +
          randomStr + "/";
      hbaseRootdir = fs.makeQualified(new Path(tmpdir));
      conf.set(HConstants.HBASE_DIR, hbaseRootdir.toString());
      fs.mkdirs(hbaseRootdir);
    } catch(IOException ioe) {
      fail("Could not create hbase root directory.");
    }

    // Start the test utility.
    testUtil = new HBaseTestingUtility(conf);
  }

  @Test
  public void testHostRank() throws Exception {

    if (System.getProperty("prop.mapred.job.tracker") != null) {
      if(LOG.isInfoEnabled())
        LOG.info("testHBaseInputOutput: Ignore this test if not local mode.");
      return;
    }

    File jarTest = new File(System.getProperty("prop.jarLocation"));
    if(!jarTest.exists()) {
      fail("Could not find Giraph jar at " +
              "location specified by 'prop.jarLocation'. " +
              "Make sure you built the main Giraph artifact?.");
    }

    MiniHBaseCluster cluster = null;
    MiniZooKeeperCluster zkCluster = null;
    FileSystem fs = null;

    try {
      // using the restart method allows us to avoid having the hbase
      // root directory overwritten by /home/$username
      zkCluster = testUtil.startMiniZKCluster();
      testUtil.restartHBaseCluster(2);
      cluster = testUtil.getMiniHBaseCluster();

      final byte[] OL_BYTES = Bytes.toBytes("ol");
      final byte[] S_BYTES = Bytes.toBytes("s");
      final byte[] METADATA_BYTES = Bytes.toBytes("mtdt");
      final byte[] TR_BYTES = Bytes.toBytes("_tr_");
      final byte[] TF_BYTES = Bytes.toBytes("_tf_");
      final byte[] TAB = Bytes.toBytes(TABLE_NAME);

      Configuration conf = cluster.getConfiguration();
      HTableDescriptor desc = new HTableDescriptor(TAB);
      desc.addFamily(new HColumnDescriptor(OL_BYTES));
      desc.addFamily(new HColumnDescriptor(S_BYTES));
      desc.addFamily(new HColumnDescriptor(METADATA_BYTES));
      HBaseAdmin hbaseAdmin=new HBaseAdmin(conf);
      if (hbaseAdmin.isTableAvailable(TABLE_NAME)) {
        hbaseAdmin.disableTable(TABLE_NAME);
        hbaseAdmin.deleteTable(TABLE_NAME);
      }
      hbaseAdmin.createTable(desc);

      /**
       * Enter the initial data
       * (a,b), (b,c), (a,c)
       * a = 1.0 - google
       * b = 1.0 - yahoo
       * c = 1.0 - bing
       */

      HTable table = new HTable(conf, TABLE_NAME);

      Put p1 = new Put(Bytes.toBytes("com.google.www"));
      p1.add(OL_BYTES, Bytes.toBytes("www.yahoo.com"), Bytes.toBytes("ab"));
      p1.add(METADATA_BYTES, TF_BYTES, Bytes.toBytes("1"));

      Put p2 = new Put(Bytes.toBytes("com.google.www"));
      p2.add(OL_BYTES, Bytes.toBytes("www.bing.com"), Bytes.toBytes("ac"));
      p2.add(OL_BYTES, Bytes.toBytes("www.bing.com"),
              Bytes.toBytes("invalid1"));
      p2.add(OL_BYTES, Bytes.toBytes("www.google.com"),
              Bytes.toBytes("invalid2"));


      Put p3 = new Put(Bytes.toBytes("com.yahoo.www"));
      p3.add(OL_BYTES, Bytes.toBytes("www.bing.com"), Bytes.toBytes("bc"));
      //p3.add(OL_BYTES, Bytes.toBytes(""), Bytes.toBytes("invalid4"));
      p3.add(METADATA_BYTES, TF_BYTES, Bytes.toBytes("1"));

      Put p4 = new Put(Bytes.toBytes("com.bing.www"));
      // TODO: Handle below case. use apache isValid method.
      p4.add(OL_BYTES, Bytes.toBytes("http://invalidurl"), Bytes.toBytes("invalid5"));
      p4.add(S_BYTES, S_BYTES, Bytes.toBytes(10.0d));
      p4.add(METADATA_BYTES, TF_BYTES, Bytes.toBytes("1"));


      Put p5 = new Put(Bytes.toBytes("dummy"));
      p5.add(S_BYTES, S_BYTES, Bytes.toBytes(10.0d));

      Put p6 = new Put(Bytes.toBytes("com.spam.www"));
      p6.add(OL_BYTES, Bytes.toBytes("www.spam2.com"), Bytes.toBytes("spamlink"));

      Put p7 = new Put(Bytes.toBytes("com.spam.www"));
      p7.add(OL_BYTES, Bytes.toBytes("www.spam3.com"), Bytes.toBytes("spamlink"));

      Put p8 = new Put(Bytes.toBytes("com.spam3.www"));
      p8.add(OL_BYTES, Bytes.toBytes("www.spam2.com"), Bytes.toBytes("spamlink"));

      Put p9 = new Put(Bytes.toBytes("com.spam4.www"));
      p9.add(OL_BYTES, Bytes.toBytes("www.spam.com"), Bytes.toBytes("spamlink"));

      table.put(p1);
      table.put(p2);
      table.put(p3);
      table.put(p4);
      table.put(p5);
      table.put(p6);
      table.put(p7);
      table.put(p8);
      table.put(p9);


      // Set Giraph configuration
      //now operate over HBase using Vertex I/O formats
      conf.set(TableInputFormat.INPUT_TABLE, TABLE_NAME);
      conf.set(TableOutputFormat.OUTPUT_TABLE, TABLE_NAME);

      // Start the giraph job
      GiraphJob giraphJob = new GiraphJob(conf, BspCase.getCallingMethodName());
      GiraphConfiguration giraphConf = giraphJob.getConfiguration();
      giraphConf.setZooKeeperConfiguration(
              cluster.getMaster().getZooKeeper().getQuorum());
      setupConfiguration(giraphJob);
      giraphConf.setComputationClass(TrustRankComputation.class);
      giraphConf.setMasterComputeClass(TrustRankVertexMasterCompute.class);
      giraphConf.setOutEdgesClass(ByteArrayEdges.class);
      giraphConf.setVertexInputFormatClass(Nutch2HostTrustInputFormat.class);
      giraphConf.setVertexOutputFormatClass(Nutch2HostOutputFormat.class);
      giraphConf.setInt("giraph.linkRank.superstepCount", 3);
      giraphConf.setInt("giraph.linkRank.scale", 10);
      giraphConf.set("giraph.linkRank.family", "mtdt");
      giraphConf.set("giraph.linkRank.qualifier", "_tr_");
      giraphConf.setFloat("giraph.linkRank.dampingFactor", 0.20f);
      giraphConf.setVertexInputFilterClass(HostRankVertexFilter.class);
      assertTrue(giraphJob.run(true));

      if(LOG.isInfoEnabled())
        LOG.info("Giraph job successful. Checking output qualifier.");

      /** Check the results **/

      Result result;
      String key;
      byte[] calculatedScoreByte;
      HashMap expectedValues = new HashMap<String, Double>();
      expectedValues.put("com.google.www", 1.3515060339386287d);
      expectedValues.put("com.yahoo.www", 4.284768409563081d);
      expectedValues.put("com.bing.www", 9.04033557353928);

       for (Object keyObject : expectedValues.keySet()){
        key = keyObject.toString();
        result = table.get(new Get(key.getBytes()));
        calculatedScoreByte = result.getValue(METADATA_BYTES, TR_BYTES);
        assertNotNull(calculatedScoreByte);
        assertTrue(calculatedScoreByte.length > 0);
       /* assertEquals("Scores are not the same",
            (Double) expectedValues.get(key),
            Bytes.toDouble(calculatedScoreByte), DELTA);    */
      }
    } finally {
      if (cluster != null) {
        cluster.shutdown();
      }
      if (zkCluster != null) {
        zkCluster.shutdown();
      }
      // clean test files
      if (fs != null) {
        fs.delete(hbaseRootdir);
      }
    }
  }
}
