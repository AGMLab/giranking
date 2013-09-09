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

import org.apache.giraph.conf.GiraphConfiguration;
import org.apache.giraph.edge.ByteArrayEdges;
import org.apache.giraph.ranking.LinkRank.*;
import org.apache.giraph.ranking.generic.LinkRankEdgeInputFormat;
import org.apache.giraph.ranking.generic.LinkRankVertexInputFormat;
import org.apache.giraph.ranking.generic.LinkRankVertexOutputFormat;
import org.apache.giraph.ranking.generic.LinkRankVertexUniformInputFormat;
import org.apache.giraph.utils.InternalVertexRunner;
import org.apache.log4j.Logger;
import org.junit.Test;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link org.apache.giraph.ranking.LinkRank.LinkRankComputation}
 */
public class LinkRankComputationTest {
  private static final Logger LOG = Logger.getLogger(LinkRankComputationTest.class);
  private static final double DELTA = 1e-3;

  @Test
  public void testToyData1() throws Exception {

    // A small graph
    String[] vertices = new String[]{
            "a 1.0",
            "b 1.0",
            "c 1.0",
    };

    String[] edges = new String[]{
            "a b",
            "b c",
            "a c",
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(LinkRankComputation.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);

    conf.setVertexInputFormatClass(LinkRankVertexInputFormat.class);
    conf.setVertexOutputFormatClass(
            LinkRankVertexOutputFormat.class);
    conf.setEdgeInputFormatClass(LinkRankEdgeInputFormat.class);
    conf.setInt("giraph.linkRank.superstepCount", 10);
    conf.setInt("giraph.linkRank.scale", 10);
    conf.setMasterComputeClass(LinkRankVertexMasterCompute.class);
    // Run internally
    Iterable<String> results = InternalVertexRunner.run(conf, vertices, edges);



    HashMap<String, Double> hm = new HashMap();
    for (String result : results) {
      String[] tokens = result.split("\t");
      hm.put(tokens[0], Double.parseDouble(tokens[1]));
      LOG.info(result);
    }

    assertEquals("a scores are not the same", 1.3515060339386287d, hm.get("a"), DELTA);
    assertEquals("b scores are not the same", 4.144902009567587d, hm.get("b"), DELTA);
    assertEquals("c scores are not the same", 9.06389778197704d, hm.get("c"), DELTA);

  }

  @Test
  public void testUniformToyData1() throws Exception {

    // A small graph
    String[] vertices = new String[]{
            "a",
            "b",
            "c",
    };

    String[] edges = new String[]{
            "a b",
            "b c",
            "a c",
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(LinkRankComputation.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);

    conf.setVertexInputFormatClass(LinkRankVertexUniformInputFormat.class);
    conf.setVertexOutputFormatClass(
            LinkRankVertexOutputFormat.class);
    conf.setEdgeInputFormatClass(LinkRankEdgeInputFormat.class);
    conf.setInt("giraph.linkRank.superstepCount", 10);
    conf.setInt("giraph.linkRank.scale", 10);
    conf.setMasterComputeClass(LinkRankVertexMasterCompute.class);
    // Run internally
    Iterable<String> results = InternalVertexRunner.run(conf, vertices, edges);



    HashMap<String, Double> hm = new HashMap();
    for (String result : results) {
      String[] tokens = result.split("\t");
      hm.put(tokens[0], Double.parseDouble(tokens[1]));
      LOG.info(result);
    }

    assertEquals("a scores are not the same", 1.3515060339386287d, hm.get("a"), DELTA);
    assertEquals("b scores are not the same", 4.144902009567587d, hm.get("b"), DELTA);
    assertEquals("c scores are not the same", 9.06389778197704d, hm.get("c"), DELTA);

  }

  @Test
  public void testToyData2() throws Exception {

    // A small graph
    String[] vertices = new String[]{
            "a 1",
            "b 1",
    };

    String[] edges = new String[]{
            "a b",
            "b a",
    };

    GiraphConfiguration conf = new GiraphConfiguration();
    conf.setComputationClass(LinkRankComputation.class);
    conf.setOutEdgesClass(ByteArrayEdges.class);

    conf.setVertexInputFormatClass(LinkRankVertexInputFormat.class);
    conf.setVertexOutputFormatClass(
            LinkRankVertexOutputFormat.class);
    conf.setEdgeInputFormatClass(LinkRankEdgeInputFormat.class);
    conf.setInt("giraph.linkRank.superstepCount", 10);
    conf.setInt("giraph.linkRank.scale", 10);
    conf.setMasterComputeClass(LinkRankVertexMasterCompute.class);
    // Run internally
    Iterable<String> results = InternalVertexRunner.run(conf, vertices, edges);

    HashMap<String, Double> hm = new HashMap();
    for (String result : results) {
      String[] tokens = result.split("\t");
      hm.put(tokens[0], Double.parseDouble(tokens[1]));
      LOG.info(result);
    }

    assertEquals("a scores are not the same", hm.get("a"), 5.0d, DELTA);
    assertEquals("b scores are not the same", hm.get("b"), 5.0d, DELTA);
  }


}
