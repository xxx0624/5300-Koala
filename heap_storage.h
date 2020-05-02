/**
 * @file heap_storage.h - Implementation of storage_engine with a heap file structure.
 * SlottedPage: DbBlock
 * HeapFile: DbFile
 * HeapTable: DbRelation
 *
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#pragma once

#include "db_cxx.h"
#include "storage_engine.h"

/**
 * @class SlottedPage - heap file implementation of DbBlock.
 *
 *      Manage a database block that contains several records.
        Modeled after slotted-page from Database Systems Concepts, 6ed, Figure 10-9.
        Record id are handed out sequentially starting with 1 as records are added with add().
        Each record has a header which is a fixed offset from the beginning of the block:
            Bytes 0x00 - Ox01: number of records
            Bytes 0x02 - 0x03: offset to end of free space
            Bytes 0x04 - 0x05: size of record 1
            Bytes 0x06 - 0x07: offset to record 1
            etc.
 *
 */
class SlottedPage : public DbBlock {
public:
    SlottedPage(Dbt &block, BlockID block_id, bool is_new = false);

    // Big 5 - use the defaults
    virtual ~SlottedPage() {}

    virtual RecordID add(const Dbt *data);

    virtual Dbt *get(RecordID record_id) const;

    virtual void put(RecordID record_id, const Dbt &data);

    virtual void del(RecordID record_id);

    virtual RecordIDs *ids(void) const;

protected:
    uint16_t num_records;
    uint16_t end_free;

    void get_header(uint16_t &size, uint16_t &loc, RecordID id = 0) const;

    void put_header(RecordID id = 0, uint16_t size = 0, uint16_t loc = 0);

    bool has_room(uint16_t size) const;

    virtual void slide(uint16_t start, uint16_t end);

    uint16_t get_n(uint16_t offset) const;

    void put_n(uint16_t offset, uint16_t n);

    void *address(uint16_t offset) const;

    friend bool test_slotted_page();
};

/**
 * @class HeapFile - heap file implementation of DbFile
 *
 * Heap file organization. Built on top of Berkeley DB RecNo file. There is one of our
        database blocks for each Berkeley DB record in the RecNo file. In this way we are using Berkeley DB
        for buffer management and file management.
        Uses SlottedPage for storing records within blocks.
 */
class HeapFile : public DbFile {
public:
    HeapFile(std::string name);

    virtual ~HeapFile() {}

    HeapFile(const HeapFile &other) = delete;

    HeapFile(HeapFile &&temp) = delete;

    HeapFile &operator=(const HeapFile &other) = delete;

    HeapFile &operator=(HeapFile &&temp) = delete;

    virtual void create(void);

    virtual void drop(void);

    virtual void open(void);

    virtual void close(void);

    virtual SlottedPage *get_new(void);

    virtual SlottedPage *get(BlockID block_id);

    virtual void put(DbBlock *block);

    virtual BlockIDs *block_ids() const;

    /**
     * Get the id of the current final block in the heap file.
     * @return block id of last block
     */
    virtual uint32_t get_last_block_id() { return last; }

protected:
    std::string dbfilename;
    uint32_t last;
    bool closed;
    Db db;

    virtual void db_open(uint flags = 0);

    virtual uint32_t get_block_count();
};


/**
 * @class HeapTable - Heap storage engine (implementation of DbRelation)
 */

class HeapTable : public DbRelation {
public:
    HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes);

    virtual ~HeapTable() {}

    HeapTable(const HeapTable &other) = delete;

    HeapTable(HeapTable &&temp) = delete;

    HeapTable &operator=(const HeapTable &other) = delete;

    HeapTable &operator=(HeapTable &&temp) = delete;

    virtual void create();

    virtual void create_if_not_exists();

    virtual void drop();

    virtual void open();

    virtual void close();

    virtual Handle insert(const ValueDict *row);

    virtual void update(const Handle handle, const ValueDict *new_values);

    virtual void del(const Handle handle);

    virtual Handles *select();

    virtual Handles *select(const ValueDict *where);

    virtual ValueDict *project(Handle handle);

    virtual ValueDict *project(Handle handle, const ColumnNames *column_names);

    using DbRelation::project;

protected:
    HeapFile file;

    virtual ValueDict *validate(const ValueDict *row) const;

    virtual Handle append(const ValueDict *row);

    virtual Dbt *marshal(const ValueDict *row) const;

    virtual ValueDict *unmarshal(Dbt *data) const;

    virtual bool selected(Handle handle, const ValueDict *where);
};


bool assertion_failure(std::string message, double x = -1, double y = -1);
bool test_slotted_page();
bool test_heap_storage();