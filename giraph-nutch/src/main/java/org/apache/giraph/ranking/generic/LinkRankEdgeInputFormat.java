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


package org.apache.giraph.ranking.generic;

import org.apache.giraph.io.EdgeReader;
import org.apache.giraph.io.formats.TextEdgeInputFormat;
import org.apache.giraph.ranking.LinkRank.utils.StringStringPair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Simple text-based {@link org.apache.giraph.io.EdgeInputFormat} for
 * unweighted graphs with Text ids.
 * <p/>
 * Each line consists of (tab delimited): source_vertex target_vertex
 */
public class LinkRankEdgeInputFormat extends
        TextEdgeInputFormat<Text, NullWritable> {
  /**
   * Splitter for endpoints
   */
  private static final Pattern SEPARATOR = Pattern.compile("[\t ]");

  @Override
  public EdgeReader<Text, NullWritable> createEdgeReader(
          InputSplit split, TaskAttemptContext context) throws IOException {
    return new TextNullTextEdgeReader();
  }

  /**
   * {@link org.apache.giraph.io.EdgeReader} associated with
   * {@link LinkRankEdgeInputFormat}.
   */
  public class TextNullTextEdgeReader extends
          TextEdgeReaderFromEachLineProcessed<StringStringPair> {
    /**
     * Current StringString pair for re-use
     */
    protected StringStringPair currentPair = new StringStringPair();

    @Override
    protected StringStringPair preprocessLine(Text line) throws IOException {
      String[] tokens = SEPARATOR.split(line.toString());
      currentPair.setFirst(tokens[0]);
      currentPair.setSecond(tokens[1]);
      return currentPair;
    }

    @Override
    protected Text getSourceVertexId(StringStringPair endpoints)
      throws IOException {
      return new Text(endpoints.getFirst());
    }

    @Override
    protected Text getTargetVertexId(StringStringPair endpoints)
      throws IOException {
      return new Text(endpoints.getSecond());
    }

    @Override
    protected NullWritable getValue(StringStringPair endpoints)
      throws IOException {
      return NullWritable.get();
    }
  }
}
