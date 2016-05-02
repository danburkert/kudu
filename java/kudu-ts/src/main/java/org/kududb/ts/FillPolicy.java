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

public enum FillPolicy {
  NONE("none"),
  ZERO("zero"),
  NOT_A_NUMBER("nan"),
  NULL("null");

  private final String name;

  FillPolicy(final String name) {
    this.name = name;
  }

  /**
   * Get this fill policy's user-friendly name.
   * @return this fill policy's user-friendly name.
   */
  public String getName() {
    return name;
  }

  /**
   * Get an instance of this enumeration from a user-friendly name.
   * @param name The user-friendly name of a fill policy.
   * @return an instance of {@link FillPolicy}, or {@code null} if the name
   * does not match any instance.
   */
  public static FillPolicy fromString(final String name) {
    for (final FillPolicy policy : FillPolicy.values()) {
      if (policy.name.equalsIgnoreCase(name)) {
        return policy;
      }
    }

    return null;
  }
}
