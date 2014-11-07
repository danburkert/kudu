// Copyright (c) 2013, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.

#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <sched.h>
#include <unistd.h>
#include <string>

#include "kudu/cfile/bloomfile.h"
#include "kudu/cfile/cfile_writer.h"
#include "kudu/gutil/stringprintf.h"
#include "kudu/gutil/sysinfo.h"
#include "kudu/util/coding.h"
#include "kudu/util/hexdump.h"
#include "kudu/util/pb_util.h"
#include "kudu/util/pthread_spinlock.h"

namespace kudu {
namespace cfile {

using fs::ReadableBlock;
using fs::ScopedWritableBlockCloser;
using fs::WritableBlock;

////////////////////////////////////////////////////////////
// Writer
////////////////////////////////////////////////////////////

BloomFileWriter::BloomFileWriter(gscoped_ptr<WritableBlock> block,
                                 const BloomFilterSizing &sizing)
  : bloom_builder_(sizing) {
  cfile::WriterOptions opts;
  opts.write_posidx = false;
  opts.write_validx = true;
  // Never use compression, regardless of the default settings, since
  // bloom filters are high-entropy data structures by their nature.
  opts.storage_attributes = ColumnStorageAttributes(PLAIN_ENCODING, NO_COMPRESSION);
  writer_.reset(new cfile::CFileWriter(opts, STRING, false, block.Pass()));
}

Status BloomFileWriter::Start() {
  return writer_->Start();
}

Status BloomFileWriter::Finish() {
  ScopedWritableBlockCloser closer;
  RETURN_NOT_OK(FinishAndReleaseBlock(&closer));
  return closer.CloseBlocks();
}

Status BloomFileWriter::FinishAndReleaseBlock(ScopedWritableBlockCloser* closer) {
  if (bloom_builder_.count() > 0) {
    RETURN_NOT_OK(FinishCurrentBloomBlock());
  }
  return writer_->FinishAndReleaseBlock(closer);
}

size_t BloomFileWriter::written_size() const {
  return writer_->written_size();
}

Status BloomFileWriter::AppendKeys(
  const Slice *keys, size_t n_keys) {

  // If this is the call on a new bloom, copy the first key.
  if (bloom_builder_.count() == 0 && n_keys > 0) {
    first_key_.assign_copy(keys[0].data(), keys[0].size());
  }

  for (size_t i = 0; i < n_keys; i++) {

    bloom_builder_.AddKey(BloomKeyProbe(keys[i]));

    // Bloom has reached optimal occupancy: flush it to the file
    if (PREDICT_FALSE(bloom_builder_.count() >= bloom_builder_.expected_count())) {
      RETURN_NOT_OK(FinishCurrentBloomBlock());

      // Copy the next key as the first key of the next block.
      // Doing this here avoids having to do it in normal code path of the loop.
      if (i < n_keys - 1) {
        first_key_.assign_copy(keys[i + 1].data(), keys[i + 1].size());
      }
    }
  }

  return Status::OK();
}

Status BloomFileWriter::FinishCurrentBloomBlock() {
  VLOG(1) << "Appending a new bloom block, first_key=" << Slice(first_key_).ToDebugString();

  // Encode the header.
  BloomBlockHeaderPB hdr;
  hdr.set_num_hash_functions(bloom_builder_.n_hashes());
  faststring hdr_str;
  PutFixed32(&hdr_str, hdr.ByteSize());
  CHECK(pb_util::AppendToString(hdr, &hdr_str));

  // The data is the concatenation of the header and the bloom itself.
  vector<Slice> slices;
  slices.push_back(Slice(hdr_str));
  slices.push_back(bloom_builder_.slice());

  // Append to the file.
  Slice start_key(first_key_);
  RETURN_NOT_OK(writer_->AppendRawBlock(slices, 0, &start_key, "bloom block"));

  bloom_builder_.Clear();

  #ifndef NDEBUG
  first_key_.assign_copy("POST_RESET");
  #endif

  return Status::OK();
}

////////////////////////////////////////////////////////////
// Reader
////////////////////////////////////////////////////////////

Status BloomFileReader::Open(gscoped_ptr<ReadableBlock> block,
                             gscoped_ptr<BloomFileReader> *reader) {
  gscoped_ptr<CFileReader> cf_reader;
  RETURN_NOT_OK(CFileReader::Open(block.Pass(), ReaderOptions(), &cf_reader));
  if (cf_reader->is_compressed()) {
    return Status::Corruption("Unexpected compression for bloom file");
  }

  gscoped_ptr<BloomFileReader> bf_reader(new BloomFileReader(cf_reader.release()));
  RETURN_NOT_OK(bf_reader->Init());

  reader->reset(bf_reader.release());
  return Status::OK();
}

BloomFileReader::BloomFileReader(CFileReader *reader)
  : reader_(reader) {
}

Status BloomFileReader::Init() {
  // The CFileReader is already initialized at this point.
  if (!reader_->has_validx()) {
    // TODO: include path!
    return Status::Corruption("bloom file missing value index");
  }

  BlockPointer validx_root = reader_->validx_root();

  // Ugly hack: create a per-cpu iterator.
  // Instead this should be threadlocal, or allow us to just
  // stack-allocate these things more smartly!
  int n_cpus = base::NumCPUs();
  for (int i = 0; i < n_cpus; i++) {
    index_iters_.push_back(
      IndexTreeIterator::Create(reader_.get(), STRING, validx_root));
  }
  iter_locks_.resize(n_cpus);

  return Status::OK();
}

Status BloomFileReader::ParseBlockHeader(const Slice &block,
                                         BloomBlockHeaderPB *hdr,
                                         Slice *bloom_data) const {
  Slice data(block);
  if (PREDICT_FALSE(data.size() < 4)) {
    return Status::Corruption("Invalid bloom block header: not enough bytes");
  }

  uint32_t header_len = DecodeFixed32(data.data());
  data.remove_prefix(sizeof(header_len));

  if (header_len > data.size()) {
    return Status::Corruption(
      StringPrintf("Header length %d doesn't fit in buffer of size %ld",
                   header_len, data.size()));
  }

  if (!hdr->ParseFromArray(data.data(), header_len)) {
    return Status::Corruption(
      string("Invalid bloom block header: ") +
      hdr->InitializationErrorString() +
      "\nHeader:" + HexDump(Slice(data.data(), header_len)));
  }

  data.remove_prefix(header_len);
  *bloom_data = data;
  return Status::OK();
}

Status BloomFileReader::CheckKeyPresent(const BloomKeyProbe &probe,
                                        bool *maybe_present) {
#if defined(linux)
  int cpu = sched_getcpu();
#else
  // Use just one lock in OSX.
  int cpu = 0;
#endif  // define(linux)
  CHECK_LT(cpu, iter_locks_.size());

  BlockPointer bblk_ptr;
  {
    boost::lock_guard<PThreadSpinLock> lock(iter_locks_[cpu]);
    cfile::IndexTreeIterator *index_iter = &index_iters_[cpu];

    Status s = index_iter->SeekAtOrBefore(probe.key());
    if (PREDICT_FALSE(s.IsNotFound())) {
      // Seek to before the first entry in the file.
      *maybe_present = false;
      return Status::OK();
    }
    RETURN_NOT_OK(s);

    // Successfully found the pointer to the bloom block. Read it.
    bblk_ptr = index_iter->GetCurrentBlockPointer();
  }

  BlockCacheHandle dblk_data;
  RETURN_NOT_OK(reader_->ReadBlock(bblk_ptr, &dblk_data));

  // Parse the header in the block.
  BloomBlockHeaderPB hdr;
  Slice bloom_data;
  RETURN_NOT_OK(ParseBlockHeader(dblk_data.data(), &hdr, &bloom_data));

  // Actually check the bloom filter.
  BloomFilter bf(bloom_data, hdr.num_hash_functions());
  *maybe_present = bf.MayContainKey(probe);
  return Status::OK();
}

} // namespace cfile
} // namespace kudu
