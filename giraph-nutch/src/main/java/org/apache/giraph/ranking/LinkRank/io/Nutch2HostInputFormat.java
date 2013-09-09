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
package org.apache.giraph.ranking.LinkRank.io;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexReader;
import org.apache.giraph.io.hbase.HBaseVertexInputFormat;
import org.apache.giraph.ranking.LinkRank.utils.NutchUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

/**
 *  HBase Input Format for HostRank.
 *  Reads edges from HBase
 *  Default table is "host" created by Nutch 2.x
 */
public class Nutch2HostInputFormat extends
    HBaseVertexInputFormat<Text, DoubleWritable, NullWritable> {

  /**
   * Logger
   */
  private static final Logger LOG =
      Logger.getLogger(Nutch2HostInputFormat.class);

  /**
   * Reusable NullWritable for edge value.
   */
  private static final NullWritable USELESS_EDGE_VALUE = NullWritable.get();

  /**
   * Creates a new VertexReader
   * @param split the split to be read
   * @param context the information about the task
   * @return VertexReader for LinkRank
   * @throws java.io.IOException
   */
  public VertexReader<Text, DoubleWritable, NullWritable>
  createVertexReader(InputSplit split,
                     TaskAttemptContext context) throws IOException {

    return new NutchTableEdgeVertexReader(split, context);

  }

  @Override
  public void checkInputSpecs(Configuration conf) {
  }

  /**
   * Uses the RecordReader to return Hbase rows
   */
  public static class NutchTableEdgeVertexReader
      extends HBaseVertexReader<Text, DoubleWritable, NullWritable> {

    /**
     * Outlink Family representative in HBase.
     * http://www.source.com - ol:http://www.target1.com, value=<Null>
     *                       - ol:http://www.target2.com, value=<Null>
     *                       - ...
     */
    private static final byte[] OUTLINK_FAMILY = Bytes.toBytes("ol");

    /**
     * Constant vertex value.
     */
    private static final DoubleWritable VERTEX_VALUE = new DoubleWritable(1.0d);

    /**
     * VertexReader for LinkRank
     * @param split InputSplit
     * @param context Context
     * @throws java.io.IOException
     */
    public NutchTableEdgeVertexReader(InputSplit split,
                                      TaskAttemptContext context)
      throws IOException {
      super(split, context);
    }

    /**
     * Returns if any vertex is remaining in the db.
     * @return if there still exists a remaining vertex.
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextVertex() throws IOException,
        InterruptedException {
      return getRecordReader().nextKeyValue();
    }

    /**
     * For each row, create a vertex with the row ID as a text,
     * and it's 'children' qualifier as a single edge.
     * @return current vertex read in the database.
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    @Override
    public Vertex<Text, DoubleWritable, NullWritable>
    getCurrentVertex()
      throws IOException, InterruptedException {
      // Get current row.
      Result row = getRecordReader().getCurrentValue();

      // Create a new vertex.
      Vertex<Text, DoubleWritable, NullWritable> vertex =
          getConf().createVertex();

      String source = NutchUtil.reverseHost(Bytes.toString(row.getRow()));

      /**
       * Get ol family map from the row.
       * Map returns:
       * Key: target1.com  Value:18
       *
       * We will convert this to {target1.com, target2.com, ... }
       */
      NavigableMap<byte[], byte[]> outlinkMap =
              row.getFamilyMap(OUTLINK_FAMILY);

      // Get the score for SourceURL (current vertex) from s:s
      //byte[] scoreByteValue = row.getValue(SCORE_FAMILY, SCORE_FAMILY);
      //Double score = Bytes.toDouble(scoreByteValue);

      // Create Writables for source URL and score value.
      Text vertexId = new Text(source);

      // Create edge list by looking at the outlinkMap.
      // Our edges are of form <TargetURL, Weight> = <Text, NullWritable>
      Set<String> targetHostSet = Sets.newHashSet();
      List<Edge<Text, NullWritable>> edges = Lists.newLinkedList();

      /**
       * Iterate over outlinkMap, add outlink urls to a set.
       **/
      Iterator it = outlinkMap.entrySet().iterator();
      while (it.hasNext()) {
        // Extract targetURL (key), Weight (value) from the key, value pair.
        NavigableMap.Entry pair = (NavigableMap.Entry) it.next();

        // Convert targetURL into Text format and add to edges list.
        String target = Bytes.toString((byte[]) pair.getKey()).trim();

        // If target is valid, add it to edges.
        if (!NutchUtil.isValidURL("http://" + target) ||
                target.equalsIgnoreCase(source)) {
          continue;
        }
        targetHostSet.add(target);
      }

      /**
       * Now convert the url string set to edge set.
       */
      for (String target : targetHostSet) {
        Text edgeId = new Text(target);
        edges.add(EdgeFactory.create(edgeId, USELESS_EDGE_VALUE));
      }

      /** With the edge list, initialize vertex with
       * sourceURL, Score and EdgeList.
       */
      vertex.initialize(vertexId, VERTEX_VALUE, edges);
      return vertex;
    }
  }
}
