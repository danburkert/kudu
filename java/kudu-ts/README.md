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

# Kudu TS

Kudu TS is a timeseries storage framework for efficiently storing timeseries
data in Kudu. It provides applications a general timeseries data model. Each
datapoint includes the following fields:

| field  | type                |
|--------|---------------------|
| metric | string              |
| tags   | map<string, string> |
| time   | timestamp           |
| value  | double              |

The `tags` field contains a map of arbitrary key value pairs. Each datapoint is
indexed on each of its included tags, which allows efficient queries which
filter by tag.
