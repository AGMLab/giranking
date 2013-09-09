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

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.VertexWriter;
import org.apache.giraph.io.hbase.HBaseVertexOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.Charset;

import static org.apache.giraph.ranking.LinkRank.utils.NutchUtil.reverseHost;


/**
 * HBase Output Format for LinkRank Computation.
 * Writes scores for each URL on the table.
 * By default, table name should be given as 'webpage'.
 */
public class Nutch2HostOutputFormat
        extends HBaseVertexOutputFormat<Text, DoubleWritable, NullWritable> {

  /**
   * Logger
   */
  private static final Logger LOG =
          Logger.getLogger(Nutch2HostOutputFormat.class);

  /**
   * HBase Vertex Writer for LinkRank
   * @param context the information about the task
   * @return NutchTableEdgeVertexWriter
   * @throws java.io.IOException
   * @throws InterruptedException
   */
  public VertexWriter<Text, DoubleWritable, NullWritable>
  createVertexWriter(TaskAttemptContext context)
    throws IOException, InterruptedException {
    return new NutchTableEdgeVertexWriter(context);
  }

  /**
   * For each vertex, write back to the configured table using
   * the vertex id as the row key bytes.
   **/
  public static class NutchTableEdgeVertexWriter
          extends HBaseVertexWriter<Text, DoubleWritable, NullWritable> {


    /**
     * Score family "s"
     */
    private static byte[] SCORE_FAMILY = Bytes.toBytes("mtdt");
    /**
     * Score qualifier "pagerank". Calculated scores will be written here.
     */
    private static byte[] LINKRANK_QUALIFIER = Bytes.toBytes("_hr_");

    /**
     * Constructor for NutchTableEdgeVertexWriter
     * @param context context
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    public NutchTableEdgeVertexWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
      super(context);
      Configuration conf = context.getConfiguration();
      String fStr = conf.get("giraph.linkRank.family", "mtdt");
      SCORE_FAMILY = Bytes.toBytes(fStr);

      String qStr = conf.get("giraph.linkRank.qualifier", "_hr_");
      LINKRANK_QUALIFIER = Bytes.toBytes(qStr);
    }

    /**
     * Write the value (score) of the vertex
     * @param vertex vertex to write
     * @throws java.io.IOException
     * @throws InterruptedException
     */
    public void writeVertex(
      Vertex<Text, DoubleWritable, NullWritable> vertex)
      throws IOException, InterruptedException {
      RecordWriter<ImmutableBytesWritable, Writable> writer = getRecordWriter();
      // get the byte representation of current vertex ID.
      String reversedUrl = reverseHost(vertex.getId().toString());
      byte[] rowBytes = reversedUrl.getBytes(Charset.forName("UTF-8"));

      // create a new Put operation with vertex value in it.
      Put put = new Put(rowBytes);

      // prepare value.
      DoubleWritable valueWritable = vertex.getValue();
      double value = valueWritable.get();
      LOG.info(reversedUrl + "=" + value);

      String valueStr = Double.toString(value);
      byte[] valueBytes = Bytes.toBytes(value);

      // write the vertex, score pair.
      if (valueStr.length() > 0) {
        put.add(SCORE_FAMILY, LINKRANK_QUALIFIER, valueBytes);
        writer.write(new ImmutableBytesWritable(rowBytes), put);
      }
    }
  }
}
