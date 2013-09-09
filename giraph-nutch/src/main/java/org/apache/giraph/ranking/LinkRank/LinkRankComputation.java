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

package org.apache.giraph.ranking.LinkRank;

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.NormalDistribution;
import org.apache.commons.math.distribution.NormalDistributionImpl;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * LinkRank Computation class. Similar to Pagerank.
 * We first remove duplicate edges and then perform pagerank calculation
 * for pre-defined steps (10).
 */
public class LinkRankComputation extends BasicComputation<Text, DoubleWritable,
        NullWritable, DoubleWritable> {

  /**
   * Number of supersteps this vertex will be involved in.
   */
  public static final String SUPERSTEP_COUNT =
          "giraph.linkRank.superstepCount";

  /**
   * Damping factor, by default 0.85.
   */
  public static final String DAMPING_FACTOR =
          "giraph.linkRank.dampingFactor";

  /**
   * Dangling score aggregator.
   */
  public static final String DANGLING_AGG = "dangling";

  /**
   * Sum of log(vertex.value)
   */
  public static final String SUM_OF_LOGS = "sumoflogs";

  /**
   * Average of log scores.
   */
  public static final String AVG_OF_LOGS = "avgoflogs";

  /**
   * Sum of deviation of log scores from logs' mean.
   */
  public static final String SUM_OF_DEVS = "sumofdevs";

  /**
   * Standard deviation
   */
  public static final String STDEV = "stdev";


  /**
   * Scale of the score. If set to 10, score will be in range [0, 10].
   */
  public static final String SCALE = "giraph.linkRank.scale";

  /**
   * Whether to attempt duplicate links or not.
   */
  public static final String REMOVE_DUPLICATES =
          "giraph.linkRank.removeDuplicates";

  /**
   * Logger.
   */
  private static final Logger LOG = Logger.getLogger(LinkRankComputation.class);

  /**
   * Maximum number of supersteps.
   */
  private int maxSteps;

  /**
   * Current superstep.
   */
  private long superStep;

  /**
   * Score scale, by default 10.
   * Meaning score will be in range [0, 10].
   */
  private int scale;

  /**
   * Damping factor, by default 0.85.
   */
  private float dampingFactor;

  /**
   * Whether to remove duplicate links.
   * By default true.
   */
  private boolean removeDuplicates;

  /**
   * Aggregated value: Average of log values of the nodes.
   */
  private DoubleWritable logAvg;

  /**
   * Aggregated value: Standard deviation.
   */
  private DoubleWritable stdev;

  /**
   * Dangling edge score contribution for each vertex
   * in the current superstep.
   */
  private Double currentDanglingContribution;

  @Override
  public void preSuperstep() {
    // Read configuration
    maxSteps = getConf().getInt(LinkRankComputation.SUPERSTEP_COUNT, 10) + 3;
    scale = getConf().getInt(LinkRankComputation.SCALE, 10);
    dampingFactor = getConf().getFloat(
            LinkRankComputation.DAMPING_FACTOR, 0.85f);
    removeDuplicates = getConf().getBoolean(
            LinkRankComputation.REMOVE_DUPLICATES, false);
    superStep = getSuperstep();

    logAvg = getAggregatedValue(LinkRankComputation.AVG_OF_LOGS);
    stdev = getAggregatedValue(LinkRankComputation.STDEV);
    currentDanglingContribution = getDanglingContribution();
  }

  /**
   * We will be receiving messages from our neighbors and process them
   * to find our new score at the new superstep.
   *
   * @param vertex   Vertex object for computation.
   * @param messages LinkRank score messages
   * @throws IOException
   */
  @Override
  public void compute(Vertex<Text, DoubleWritable, NullWritable> vertex,
                      Iterable<DoubleWritable> messages)
    throws IOException {

    receiveMessages(vertex, messages);
    distributeScores(vertex);
    if (superStep >= maxSteps - 4) {
      normalizeVertexScore(vertex);
    }
  }

  /**
   * Receives messages from neighbors and sets new score.
   * @param vertex current vertex
   * @param messages messages from neighbors
   */
  private void receiveMessages(
    Vertex<Text, DoubleWritable, NullWritable> vertex,
    Iterable<DoubleWritable> messages) {
    double sum = 0.0d;

    if (superStep == 0) {
      if (removeDuplicates) {
        removeDuplicateLinks(vertex);
      }
    } else if (1 <= superStep && superStep <= maxSteps - 4) {
      // find the score sum received from our neighbors.
      for (DoubleWritable message : messages) {
        sum += message.get();
      }

      Double newValue =
        ((1f - dampingFactor) / getTotalNumVertices()) +
                dampingFactor * (sum + currentDanglingContribution);

      vertex.setValue(new DoubleWritable(newValue));
      LOG.info(vertex.getId().toString() + "=" + vertex.getValue().get());

    }
  }

  /**
   * Normalize the vertex scores.
   * Uses log scale and cumulative probability distribution.
   * @param vertex current vertex
   */
  private void normalizeVertexScore(
    Vertex<Text, DoubleWritable, NullWritable> vertex) {

    double logValueDouble = Math.log(vertex.getValue().get());
    if (superStep == maxSteps - 4) {
      /**
       * Calculate LOG(value) and aggregate to SUM_OF_LOGS.
       */
      DoubleWritable logValue = new DoubleWritable(logValueDouble);
      aggregate(LinkRankComputation.SUM_OF_LOGS, logValue);
    } else if (superStep == maxSteps - 2) {
      /** Pass previous superstep since WorkerContext will need SUM_OF_LOGS
       *  to be aggregated.
       *  In this step, get AVG_OF_LOGS (calculated by WorkerContext)
       *  and calculate meanSquareError, aggregate it to SUM_OF_DEVS.
       *  WorkerContext will use SUM_OF_DEVS to calculate stdev
       *  in maxsupersteps-1 step.
       */
      double meanSquareError = Math.pow(logValueDouble - logAvg.get(), 2);
      DoubleWritable mseWritable = new DoubleWritable(meanSquareError);
      aggregate(LinkRankComputation.SUM_OF_DEVS, mseWritable);
    } else if (superStep == maxSteps) {
      /**
       * Pass maxsupersteps-1 step since WorkerContext will calculate stdev.
       * Use stdev and AVG_OF_LOGS to create a Normal Distribution.
       * Calculate CDF, scale it and set the new value.
       */
      double newValue = 1.0d;
      double stdevValue = stdev.get();
      if (stdevValue == 0.0d) {
        stdevValue = 1e-10;
      }

      NormalDistribution dist = new NormalDistributionImpl(
              logAvg.get(), stdevValue);
      try {
        double cumProb = dist.cumulativeProbability(logValueDouble);
        newValue = cumProb * scale;
      } catch (MathException e) {
        e.printStackTrace();
      }
      vertex.setValue(new DoubleWritable(newValue));
    }
  }


  /**
   * If we are at a superstep that is not the last one,
   * send messages to the neighbors.
   * If it's the last step, vote to halt!
   *
   * @param vertex will distribute vertex's score to its neighbors.
   */
  private void distributeScores(
    Vertex<Text, DoubleWritable, NullWritable> vertex) {
    int edgeCount = vertex.getNumEdges();

    if (superStep < maxSteps) {
      DoubleWritable message = new DoubleWritable(
              vertex.getValue().get() / edgeCount
      );

      if (edgeCount == 0) {
        aggregate(LinkRankComputation.DANGLING_AGG, vertex.getValue());
      } else {
        sendMessageToAllEdges(vertex, message);
      }
    } else {
      vertex.voteToHalt();
    }
  }

  /**
   * Calculates dangling node score contribution for each individual node.
   *
   * @return score to give each individual node
   */
  public Double getDanglingContribution() {
    DoubleWritable danglingWritable =
            getAggregatedValue(LinkRankComputation.DANGLING_AGG);
    Double danglingSum = danglingWritable.get();
    Double contribution = danglingSum / getTotalNumVertices();
    return contribution;
  }

  /**
   * Removes duplicate outgoing links.
   *
   * @param vertex vertex whose duplicate outgoing edges
   *               will be removed.
   */
  public void removeDuplicateLinks(Vertex<Text, DoubleWritable,
          NullWritable> vertex) {
    String sourceUrl = vertex.getId().toString().trim();
    String targetUrl;
    Set<String> urls = new HashSet<String>();

    Iterable<Edge<Text, NullWritable>> outgoingEdges = vertex.getEdges();

    for (Edge<Text, NullWritable> edge : outgoingEdges) {
      targetUrl = edge.getTargetVertexId().toString().trim().split("#")[0];
      // if source != target (avoid self-links)
      if (!targetUrl.equalsIgnoreCase(sourceUrl)) {
        urls.add(targetUrl);
      }
    }

    ArrayList<Edge<Text, NullWritable>> newEdges =
            new ArrayList<Edge<Text, NullWritable>>();
    for (final String url : urls) {
      newEdges.add(new Edge<Text, NullWritable>() {
        @Override
        public Text getTargetVertexId() {
          return new Text(url);
        }

        @Override
        public NullWritable getValue() {
          return NullWritable.get();
        }
      });
    }

    if (newEdges.size() > 0) {
      vertex.setEdges(newEdges);
    }
  }
}
