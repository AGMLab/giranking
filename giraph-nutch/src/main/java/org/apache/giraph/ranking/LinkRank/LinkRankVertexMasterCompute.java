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

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.master.MasterCompute;
import org.apache.hadoop.io.DoubleWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Master compute associated with {@link LinkRankComputation}.
 * It registers required aggregators.
 */
public class LinkRankVertexMasterCompute extends
  MasterCompute {
  @Override
  public void compute() {
    // add additional 3 steps for normalization.
    long maxSteps = getContext().getConfiguration().
            getLong(LinkRankComputation.SUPERSTEP_COUNT, 10) + 3;
    long superstep = getSuperstep();
    if (superstep == maxSteps - 2) {
        /**
         * We should have log values of the scores aggregated in SUM_OF_LOGS.
         * Divide this sum by total number of vertices and aggragate in
         * AVG_OF_LOGS.
         */
      DoubleWritable logsum =
              getAggregatedValue(LinkRankComputation.SUM_OF_LOGS);
      DoubleWritable avg = new DoubleWritable(
              logsum.get() / getTotalNumVertices());

      setAggregatedValue(LinkRankComputation.AVG_OF_LOGS, avg);

    } else if (superstep == maxSteps) {
      /**
       * Calculate standart deviation with deviation sums SUM_OF_DEVS.
       * Aggregate result to STDEV.
       */
      DoubleWritable devSum =
              getAggregatedValue(LinkRankComputation.SUM_OF_DEVS);
      double ratio = devSum.get() / getTotalNumVertices();
      DoubleWritable stdev = new DoubleWritable(Math.sqrt(ratio));
      setAggregatedValue(LinkRankComputation.STDEV, stdev);
    }
  }

  @Override
  public void initialize() throws InstantiationException,
    IllegalAccessException {
    registerPersistentAggregator(
            LinkRankComputation.SUM_OF_LOGS, DoubleSumAggregator.class);
    registerPersistentAggregator(
            LinkRankComputation.SUM_OF_DEVS, DoubleSumAggregator.class);
    registerPersistentAggregator(
            LinkRankComputation.AVG_OF_LOGS, DoubleSumAggregator.class);
    registerPersistentAggregator(
            LinkRankComputation.STDEV, DoubleSumAggregator.class);

    registerAggregator(LinkRankComputation.DANGLING_AGG,
            DoubleSumAggregator.class);
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {

  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {

  }
}
