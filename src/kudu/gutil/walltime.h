// Copyright 2012 Google Inc. All Rights Reserved.
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
#ifndef GUTIL_WALLTIME_H_
#define GUTIL_WALLTIME_H_

#include <sys/time.h>

#include <string>
using std::string;

#if defined(__APPLE__)
#include <mach/clock.h>
#include <mach/mach.h>
#endif  // defined(__APPLE__)

#include "kudu/gutil/integral_types.h"

typedef double WallTime;

// Append result to a supplied string.
// If an error occurs during conversion 'dst' is not modified.
void StringAppendStrftime(std::string* dst,
                                 const char* format,
                                 time_t when,
                                 bool local);

// Return the local time as a string suitable for user display.
std::string LocalTimeAsString();

// Similar to the WallTime_Parse, but it takes a boolean flag local as
// argument specifying if the time_spec is in local time or UTC
// time. If local is set to true, the same exact result as
// WallTime_Parse is returned.
bool WallTime_Parse_Timezone(const char* time_spec,
                                    const char* format,
                                    const struct tm* default_time,
                                    bool local,
                                    WallTime* result);

// Return current time in seconds as a WallTime.
WallTime WallTime_Now();

typedef int64 MicrosecondsInt64;

namespace walltime_internal {

#if defined(linux)
inline MicrosecondsInt64 GetClockTimeMicros(clockid_t clock) {
  timespec ts;
  clock_gettime(clock, &ts);
  return ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

#elif defined(__APPLE__)
inline MicrosecondsInt64 GetClockTimeMicros() {
  clock_serv_t cclock;
  mach_timespec_t ts;
  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
  clock_get_time(cclock, &ts);
  mach_port_deallocate(mach_task_self(), cclock);
  return ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}
#endif  // defined(linux)

} // namespace walltime_internal

// Returns the time since the Epoch measured in microseconds.
inline MicrosecondsInt64 GetCurrentTimeMicros() {
#if defined(linux)
  return walltime_internal::GetClockTimeMicros(CLOCK_REALTIME);
#elif defined(__APPLE__)
  return walltime_internal::GetClockTimeMicros();
#endif  // defined(linux)
}

// Returns the time since some arbitrary reference point, measured in microseconds.
// Guaranteed to be monotonic (and therefore useful for measuring intervals)
inline MicrosecondsInt64 GetMonoTimeMicros() {
#if defined(linux)
  return walltime_internal::GetClockTimeMicros(CLOCK_MONOTONIC);
#elif defined(__APPLE__)
  return walltime_internal::GetClockTimeMicros();
#endif  // defined(linux)
}

// Returns the time spent in user CPU on the current thread, measured in microseconds.
inline MicrosecondsInt64 GetThreadCpuTimeMicros() {
#if defined(linux)
  return walltime_internal::GetClockTimeMicros(CLOCK_THREAD_CPUTIME_ID);
#elif defined(__APPLE__)
  return walltime_internal::GetClockTimeMicros();
#endif  // defined(linux)
}

// A CycleClock yields the value of a cycle counter that increments at a rate
// that is approximately constant.
class CycleClock {
 public:
  // Return the value of the counter.
  static inline int64 Now();

 private:
  CycleClock();
};

#include "kudu/gutil/cycleclock-inl.h"  // inline method bodies
#endif  // GUTIL_WALLTIME_H_
