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

package org.apache.giraph.ranking.LinkRank.utils;

/**
 * A pair of Strings.
 */
public class StringStringPair {
  /** First element. */
  private String first;
  /** Second element. */
  private String second;

  /**
   * Emtpy constructor.
   */
  public StringStringPair() {

  }

  /** Constructor.
   *
   * @param fst First element
   * @param snd Second element
   */
  public StringStringPair(String fst, String snd) {
    first = fst;
    second = snd;
  }

  /**
   * Get the first element.
   *
   * @return The first element
   */
  public String getFirst() {
    return first;
  }

  /**
   * Set the first element.
   *
   * @param first The first element
   */
  public void setFirst(String first) {
    this.first = first;
  }

  /**
   * Get the second element.
   *
   * @return The second element
   */
  public String getSecond() {
    return second;
  }

  /**
   * Set the second element.
   *
   * @param second The second element
   */
  public void setSecond(String second) {
    this.second = second;
  }
}
