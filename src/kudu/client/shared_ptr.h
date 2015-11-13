// Copyright 2015 Cloudera, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#ifndef KUDU_CLIENT_SHARED_PTR_H
#define KUDU_CLIENT_SHARED_PTR_H

#if defined(__APPLE__)
#include <memory>

namespace kudu {
namespace client {
namespace sp {
  using std::shared_ptr;
  using std::weak_ptr;
  using std::enable_shared_from_this;
}
}
}

#else
#include <tr1/memory>

namespace kudu {
namespace client {
namespace sp {
  using std::tr1::shared_ptr;
  using std::tr1::weak_ptr;
  using std::tr1::enable_shared_from_this;
}
}
}
#endif

#endif // define KUDU_CLIENT_SHARED_PTR_H
