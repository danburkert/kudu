/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.kududb.ts;

import com.google.common.base.Objects;

import javax.annotation.concurrent.NotThreadSafe;

import org.kududb.annotations.InterfaceAudience;
import org.kududb.annotations.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Unstable
@NotThreadSafe
public final class Datapoint {
  private long time;
  private double value;

  private Datapoint(long time, double value) {
    this.time = time;
    this.value = value;
  }

  /**
   * Creates a new {@code Datapoint} with the provided time and value.
   * @param microseconds the {@code Datapoint}'s time in microseconds
   * @param value the {@code Datapoint}'s value
   * @return a new {@code Datapoint}
   */
  public static Datapoint create(long microseconds, double value) {
    return new Datapoint(microseconds, value);
  }

  /**
   * Returns the {@code Datapoint}'s time in microseconds.
   * @return the {@code Datapoint}'s time in microseconds
   */
  public long getTime() {
    return time;
  }

  /**
   * Returns the {@code Datapoint}'s value.
   * @return the {@code Datapoint}'s value
   */
  public double getValue() {
    return value;
  }

  /**
   * Sets the time of the {@code Datapoint}.
   * @param microseconds the new time in microseconds
   */
  public void setTime(long microseconds) {
    time = microseconds;
  }

  /**
   * Sets the value of the {@code Datapoint}.
   * @param value the new value
   */
  public void setValue(double value) {
    this.value = value;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    return Objects.toStringHelper(this)
                  .add("time", time)
                  .add("value", value)
                  .toString();
  }
}
