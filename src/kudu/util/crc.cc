// Copyright (c) 2014 Cloudera Inc.
// Confidential Cloudera Information: Covered by NDA.
#include "kudu/util/crc.h"

#include <crcutil/interface.h>
#include <gperftools/heap-checker.h>

#include "kudu/gutil/once.h"

namespace kudu {
namespace crc {

static GoogleOnceType crc32c_once = GOOGLE_ONCE_INIT;
static Crc* crc32c_instance = NULL;

static void InitCrc32cInstance() {
#if defined(TCMALLOC_ENABLED) && !defined(__APPLE__)
  HeapLeakChecker::Disabler disabler; // CRC instance is never freed.
#endif // TCMALLOC_ENABLED && !defined(__APPLE__)
  // TODO: Is initial = 0 and roll window = 4 appropriate for all cases?
  crc32c_instance = crcutil_interface::CRC::CreateCrc32c(true, 0, 4, NULL);
}

Crc* GetCrc32cInstance() {
  GoogleOnceInit(&crc32c_once, &InitCrc32cInstance);
  return crc32c_instance;
}

uint32_t Crc32c(const void* data, size_t length) {
  uint64_t crc32 = 0;
  GetCrc32cInstance()->Compute(data, length, &crc32);
  return static_cast<uint32_t>(crc32); // Only uses lower 32 bits.
}

} // namespace crc
} // namespace kudu
