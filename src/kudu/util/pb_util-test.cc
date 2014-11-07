// Copyright (c) 2013, Cloudera,inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/assign.hpp>
#include <boost/foreach.hpp>
#include <gtest/gtest.h>
#include <string>
#include <sys/types.h>
#include <tr1/memory>
#include <unistd.h>
#include <vector>

#include "kudu/gutil/gscoped_ptr.h"
#include "kudu/util/env_util.h"
#include "kudu/util/memenv/memenv.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/pb_util-internal.h"
#include "kudu/util/proto_container_test.pb.h"
#include "kudu/util/status.h"
#include "kudu/util/test_util.h"

namespace kudu {
namespace pb_util {

using std::string;
using std::tr1::shared_ptr;
using std::vector;
using internal::WritableFileOutputStream;

static const char* kTestFileName = "pb_container.meta";
static const char* kTestMagic = "testcont";
static const char* kTestKeyvalName = "my-key";
static const int kTestKeyvalValue = 1;

class TestPBUtil : public KuduTest {
 public:
  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    path_ = GetTestPath(kTestFileName);
  }

 protected:
  // Create a container file with expected values.
  // Since this is a unit test class, and we want it to be fast, we do not
  // fsync by default.
  Status CreateKnownGoodContainerFile(SyncMode sync = NO_SYNC);

  // XORs the data in the specified range of the file at the given path.
  Status BitFlipFileByteRange(const string& path, uint64_t offset, uint64_t length);

  // Output file name for most unit tests.
  string path_;
};

Status TestPBUtil::CreateKnownGoodContainerFile(SyncMode sync) {
  ProtoContainerTestPB test_pb;
  test_pb.set_name(kTestKeyvalName);
  test_pb.set_value(kTestKeyvalValue);
  return WritePBContainerToPath(env_.get(), path_, kTestMagic, test_pb, sync);
}

Status TestPBUtil::BitFlipFileByteRange(const string& path, uint64_t offset, uint64_t length) {
  faststring buf;
  // Read the data from disk.
  {
    gscoped_ptr<RandomAccessFile> file;
    RETURN_NOT_OK(env_->NewRandomAccessFile(path, &file));
    size_t size;
    RETURN_NOT_OK(file->Size(&size));
    Slice slice;
    faststring scratch(size);
    RETURN_NOT_OK(env_util::ReadFully(file.get(), 0, size, &slice, scratch.data()));
    buf.append(slice.data(), slice.size());
  }

  // Flip the bits.
  for (uint64_t i = 0; i < length; i++) {
    uint8_t* addr = buf.data() + offset + i;
    *addr = ~*addr;
  }

  // Write the data back to disk.
  gscoped_ptr<WritableFile> file;
  RETURN_NOT_OK(env_->NewWritableFile(path, &file));
  RETURN_NOT_OK(file->Append(buf));
  RETURN_NOT_OK(file->Close());

  return Status::OK();
}

TEST_F(TestPBUtil, TestWritableFileOutputStream) {
  gscoped_ptr<Env> env(NewMemEnv(Env::Default()));
  shared_ptr<WritableFile> file;
  ASSERT_STATUS_OK(env_util::OpenFileForWrite(env.get(), "/test", &file));

  WritableFileOutputStream stream(file.get(), 4096);

  void* buf;
  int size;

  // First call should yield the whole buffer.
  ASSERT_TRUE(stream.Next(&buf, &size));
  ASSERT_EQ(4096, size);
  ASSERT_EQ(4096, stream.ByteCount());

  // Backup 1000 and the next call should yield 1000
  stream.BackUp(1000);
  ASSERT_EQ(3096, stream.ByteCount());

  ASSERT_TRUE(stream.Next(&buf, &size));
  ASSERT_EQ(1000, size);

  // Another call should flush and yield a new buffer of 4096
  ASSERT_TRUE(stream.Next(&buf, &size));
  ASSERT_EQ(4096, size);
  ASSERT_EQ(8192, stream.ByteCount());

  // Should be able to backup to 7192
  stream.BackUp(1000);
  ASSERT_EQ(7192, stream.ByteCount());

  // Flushing shouldn't change written count.
  ASSERT_TRUE(stream.Flush());
  ASSERT_EQ(7192, stream.ByteCount());

  // Since we just flushed, we should get another full buffer.
  ASSERT_TRUE(stream.Next(&buf, &size));
  ASSERT_EQ(4096, size);
  ASSERT_EQ(7192 + 4096, stream.ByteCount());

  ASSERT_TRUE(stream.Flush());

  ASSERT_EQ(stream.ByteCount(), file->Size());
}

// Basic read/write test.
TEST_F(TestPBUtil, TestPBContainerSimple) {
  // Exercise both the SYNC and NO_SYNC codepaths, despite the fact that we
  // aren't able to observe a difference in the test.
  vector<SyncMode> modes = boost::assign::list_of(SYNC)(NO_SYNC);
  BOOST_FOREACH(SyncMode mode, modes) {

    // Write the file.
    ASSERT_OK(CreateKnownGoodContainerFile(mode));

    // Read it back, should validate and contain the expected values.
    ProtoContainerTestPB test_pb;
    ASSERT_OK(ReadPBContainerFromPath(env_.get(), path_, kTestMagic, &test_pb));
    ASSERT_EQ(kTestKeyvalName, test_pb.name());
    ASSERT_EQ(kTestKeyvalValue, test_pb.value());

    // Delete the file.
    ASSERT_OK(env_->DeleteFile(path_));
  }
}

// Corruption / various failure mode test.
TEST_F(TestPBUtil, TestPBContainerCorruption) {
  // Test that we indicate when the file does not exist.
  ProtoContainerTestPB test_pb;
  Status s = ReadPBContainerFromPath(env_.get(), path_, kTestMagic, &test_pb);
  ASSERT_TRUE(s.IsNotFound()) << "Should not be found: " << path_ << ": " << s.ToString();

  // Test that an empty file looks like corruption.
  {
    // Create the empty file.
    gscoped_ptr<WritableFile> file;
    ASSERT_OK(env_->NewWritableFile(path_, &file));
    ASSERT_OK(file->Close());
  }
  s = ReadPBContainerFromPath(env_.get(), path_, kTestMagic, &test_pb);
  ASSERT_TRUE(s.IsCorruption()) << "Should be zero length: " << path_ << ": " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "File size not large enough to be valid");

  // Test truncated file.
  ASSERT_OK(CreateKnownGoodContainerFile());
  uint64_t known_good_size = 0;
  ASSERT_OK(env_->GetFileSize(path_, &known_good_size));
  int ret = truncate(path_.c_str(), known_good_size - 2);
  if (ret != 0) {
    PLOG(ERROR) << "truncate() of file " << path_ << " failed";
    FAIL();
  }
  s = ReadPBContainerFromPath(env_.get(), path_, kTestMagic, &test_pb);
  ASSERT_TRUE(s.IsCorruption()) << "Should be incorrect size: " << path_ << ": " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Incorrect file size");

  // Test corrupted magic.
  ASSERT_OK(CreateKnownGoodContainerFile());
  ASSERT_OK(BitFlipFileByteRange(path_, 0, 2));
  s = ReadPBContainerFromPath(env_.get(), path_, kTestMagic, &test_pb);
  ASSERT_TRUE(s.IsCorruption()) << "Should have invalid magic: " << path_ << ": " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Invalid magic number");

  // Test corrupted version.
  ASSERT_OK(CreateKnownGoodContainerFile());
  ASSERT_OK(BitFlipFileByteRange(path_, 8, 2));
  s = ReadPBContainerFromPath(env_.get(), path_, kTestMagic, &test_pb);
  ASSERT_TRUE(s.IsNotSupported()) << "Should have unsupported version number: " << path_ << ": "
                                  << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "we only support version 1");

  // Test corrupted size.
  ASSERT_OK(CreateKnownGoodContainerFile());
  ASSERT_OK(BitFlipFileByteRange(path_, 12, 2));
  s = ReadPBContainerFromPath(env_.get(), path_, kTestMagic, &test_pb);
  ASSERT_TRUE(s.IsCorruption()) << "Should be incorrect size: " << path_ << ": " << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Incorrect file size");

  // Test corrupted data (looks like bad checksum).
  ASSERT_OK(CreateKnownGoodContainerFile());
  ASSERT_OK(BitFlipFileByteRange(path_, 16, 2));
  s = ReadPBContainerFromPath(env_.get(), path_, kTestMagic, &test_pb);
  ASSERT_TRUE(s.IsCorruption()) << "Should be incorrect checksum: " << path_ << ": "
                                << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Incorrect checksum");

  // Test corrupted checksum.
  ASSERT_OK(CreateKnownGoodContainerFile());
  ASSERT_OK(BitFlipFileByteRange(path_, known_good_size - 4, 2));
  s = ReadPBContainerFromPath(env_.get(), path_, kTestMagic, &test_pb);
  ASSERT_TRUE(s.IsCorruption()) << "Should be incorrect checksum: " << path_ << ": "
                                << s.ToString();
  ASSERT_STR_CONTAINS(s.ToString(), "Incorrect checksum");
}

} // namespace pb_util
} // namespace kudu
