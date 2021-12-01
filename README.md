# Nouzdb
An embedded database for learning purpose that is based on SSTables.

## Plan
- [x] Implement a memtable.
- [x] Implement the write-ahead logging mechanism for the memtable.
- [x] Implement errors detection and recovery process for write-ahead log.
- [ ] Write out to a sorted-string segement file when the memtable grows too big, and create a new one.
- [ ] Allow searching for keys in sorted-string segement files.
- [ ] Implement a merging process for the segement files.
- [ ] Build in-memory indexes for every segement files.
- [ ] Make the indexes be sparse to save memory by seperating the segement files into blocks and picking the index of the smallest key in each block.
- [ ] Compress the blocks before writing to the disk.
