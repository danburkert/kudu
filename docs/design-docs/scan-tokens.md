<!---
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Scan Token API

## Motivation

Most distributed compute frameworks that integrate with Kudu need the ability to
split Kudu tables into physical sections so that computation can be distributed
and parallelized. Additionally, these frameworks often want to take advantage of
data locality by executing tasks on or close to the physical machines that hold
the data they are working on.

Kudu should have a well defined API that all compute integrations can use to
implement parallel scanning with locality hints.

## Design

Kudu will provide a client API that takes a scan description (e.g. table name,
projected columns, fault tolerance, snapshot timestamp, lower and upper primary
key bounds, predicates, etc.) and returns a sequence of scan tokens. For
example:

```java
ScanTokenBuilder builder = client.newScanTokenBuilder();
builder.setProjectedColumnNames(ImmutableList.of("col1", "col2"));
List<ScanToken> tokens = builder.build();
```

Scan tokens may be used to create a scanner over a single tablet. Additionally,
scan tokens have a well defined, but opaque to the client, serialization format
so that tokens may be serialized and deserialized by the compute framework, and
even passed between processes using different Kudu client versions or
implementations (JVM vs. C++). Continuing the previous example:

```java
ByteBuffer serializedToken = tokens.get(0).serialize();

// later, possibly in a different process

KuduScanner scanner = ScanToken.deserializeScanToken(serializedToken, client);
```

Along with the serializable scan token, the API will provide a location hint
containing the replicas hosting the data. This will be done via the existing
replica location APIs (`org.kududb.client.LocatedTablet` in the Java client, and
`std::vector<KuduTabletServer*>` in the C++ client).

Scan tokens should be splittable so that intra-tablet parallelization can be
achieved. The client is better able to estimate appropriate primary key bounds
without requiring exposure of internal metrics in public client interfaces.
