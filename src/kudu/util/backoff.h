// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "kudu/util/monotime.h"

namespace kudu {

// A randomized exponential backoff policy for retrying operations.
//
// See [1] and [2] for more discussion.
//
// [1][Backoff in Distributed Systems]
//    (http://dthain.blogspot.com/2009/02/exponential-backoff-in-distributed.html)
// [2][Exponential Backoff and Jitter](https://www.awsarchitectureblog.com/2015/03/backoff.html)
class Backoff {
 public:

  const MonoDelta& base() const {
    return base_;

  }

 private:

  MonoDelta base_;

}

}
