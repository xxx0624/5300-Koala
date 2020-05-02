#include "heap_storage.h"
#include "storage_engine.h"
#include "db_cxx.h"
#include <cstring>
#include <vector>
#include <exception>
using namespace std;

/**
 * Print out given failure message and return false.
 * @param message reason for failure
 * @return false
 */
bool assertion_failure(string message) {
    cout << "FAILED TEST: " << message << endl;
    return false;
}

/**
 * Testing function for SlottedPage.
 * @return true if testing succeeded, false otherwise
 */
bool test_slotted_page() {
    // construct one
    char blank_space[DbBlock::BLOCK_SZ];
    Dbt block_dbt(blank_space, sizeof(blank_space));
    SlottedPage slot(block_dbt, 1, true);
    // add a record
    char rec1[] = "hello";
    Dbt rec1_dbt(rec1, sizeof(rec1));
    RecordID id = slot.add(&rec1_dbt);
    if (id != 1)
        return assertion_failure("add id 1");
    // get it back
    Dbt *get_dbt = slot.get(id);
    string expected(rec1, sizeof(rec1));
    string actual((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 1 back " + actual);
    delete get_dbt;
    // add another record and fetch it back
    char rec2[] = "goodbye";
    Dbt rec2_dbt(rec2, sizeof(rec2));
    id = slot.add(&rec2_dbt);
    if (id != 2)
        return assertion_failure("add id 2");
    // get it back
    get_dbt = slot.get(id);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 2 back " + actual);
    delete get_dbt;
    // test put with expansion (and slide and ids)
    char rec1_rev[] = "something much bigger";
    rec1_dbt = Dbt(rec1_rev, sizeof(rec1_rev));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after expanding put
    get_dbt = slot.get(2);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 2 back after expanding put of 1 " + actual);
    delete get_dbt;
    get_dbt = slot.get(1);
    expected = string(rec1_rev, sizeof(rec1_rev));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 1 back after expanding put of 1 " + actual);
    delete get_dbt;
    // test put with contraction (and slide and ids)
    rec1_dbt = Dbt(rec1, sizeof(rec1));
    slot.put(1, rec1_dbt);
    // check both rec2 and rec1 after contracting put
    get_dbt = slot.get(2);
    expected = string(rec2, sizeof(rec2));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 2 back after contracting put of 1 " + actual);
    delete get_dbt;
    get_dbt = slot.get(1);
    expected = string(rec1, sizeof(rec1));
    actual = string((char *) get_dbt->get_data(), get_dbt->get_size());
    if (expected != actual)
        return assertion_failure("get 1 back after contracting put of 1 " + actual);
    delete get_dbt;
    // test del (and ids)
    RecordIDs *id_list = slot.ids();
    if (id_list->size() != 2 || id_list->at(0) != 1 || id_list->at(1) != 2)
        return assertion_failure("ids() with 2 records");
    delete id_list;
    slot.del(1);
    id_list = slot.ids();
    if (id_list->size() != 1 || id_list->at(0) != 2)
        return assertion_failure("ids() with 1 record remaining");
    delete id_list;
    get_dbt = slot.get(1);
    if (get_dbt != nullptr)
        return assertion_failure("get of deleted record was not null");
    // try adding something too big
    rec2_dbt = Dbt(nullptr, DbBlock::BLOCK_SZ - 10); // too big, but only because we have a record in there
    try {
        slot.add(&rec2_dbt);
        return assertion_failure("failed to throw when add too big");
    } catch (const DbBlockNoRoomError &exc) {
        // test succeeded - this is the expected path
    } catch (...) {
        // Note that this won't catch segfault signals -- but in that case we also know the test failed
        return assertion_failure("wrong type thrown when add too big");
    }
    return true;
}

bool test_heap_storage() {
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_table", column_names, column_attributes);
    table1.create();
    /*    	cout << "create ok" << endl;
       	 table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
        	cout << "drop ok" << endl;

        	HeapTable table("_test_data_cpp", column_names, column_attributes);
       	 table.create_if_not_exists();
        	cout << "create_if_not_exsts ok" << endl;

        	ValueDict row;
        	row["a"] = Value(12);
        	row["b"] = Value("Hello!");
        	cout << "try insert" <<endl;
        	table.insert(&row);
        	cout << "insert ok" << endl;
        	Handles* handles = table.select();
        	cout << "select ok " << handles->size() << endl;
        	ValueDict *result = table.project((*handles)[0]);
        	cout << "project ok" << endl;
        	Value value = (*result)["a"];
           	if (value.n != 12)
        		return false;
        	value = (*result)["b"];
        	if (value.s != "Hello!")
    		return false;
        	table.drop();	*/
    return true;
}
//Slotted page stuff here
// need del,get put, ids, get_header, has_room, slide

typedef u_int16_t u16;

// SlottedPage Constructure

SlottedPage::SlottedPage(Dbt& block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt* data) {
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16)data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

// Get a record from the block.
Dbt* SlottedPage::get(RecordID record_id) {
    u16 size, loc;
    get_header(size, loc, record_id);
    if (loc == 0) {
        return nullptr;
    }
    return new Dbt(this->address(loc), size);
}


// Replace the record with the given data.
void SlottedPage::put(RecordID record_id, const Dbt& data) {
    u16 size, loc;
    get_header(size, loc, record_id);
    u16 new_size = (u16)data.get_size();
    if (new_size > size) {
        u16 extra = new_size - size;
        if (!has_room(extra)) {
            throw DbBlockNoRoomError("Not having room for enlarged record.");
        }
        slide(loc, loc - extra);
        memcpy(this->address(loc - extra), data.get_data(), new_size);
    } else {
        memcpy(this->address(loc), data.get_data(), new_size);
        slide(loc + new_size, loc + size);
    }
    get_header(size, loc, record_id);
    put_header(record_id, new_size, loc);
}


// Mark the given record_id as deleted by changing its size to zero and its location to 0.
void SlottedPage::del(RecordID record_id) {
    u16 size, loc;
    get_header(size, loc, record_id);
    put_header(record_id, 0, 0);
    slide(loc, loc + size);
}

// Sequence of all non-deleted record ids
RecordIDs* SlottedPage::ids(void) {
    u16 size, loc;
    RecordIDs* ids = {};
    for (int i = 1; i < this->num_records + 1; i++) {
        get_header(size, loc, i);
        if (size != 0 || loc != 0) {
            ids->push_back(i);
        }
    }
    return ids;
}


// Modify the size and offset for given record_id.
void SlottedPage::get_header(u_int16_t& size, u_int16_t& loc, RecordID id) {
    size = get_n(4 * id);
    loc = get_n(4 * id + 2);
}


// Put the size and offset for given record_id.
void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc) {
    if (id == 0) {
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4 * id, size);
    put_n(4 * id + 2, loc);
}


// Check if there is room to store a record with given size
bool SlottedPage::has_room(u_int16_t size) {
    u16 available = this->end_free - (this->num_records + 2) * 4;
    return size <= available;
}


// If start < end, then remove data from offset start up to but not including offset end by sliding data
// that is to the left of start to the right.If start > end, then make room for extra data from end to start
// by sliding data that is to the left of start to the left.
// Also fix up any record headers whose data has slid.Assumes there is enough room if it is a left
// shift(end < start)
void SlottedPage::slide(u_int16_t start, u_int16_t end) {
    u16 shift = end - start;
    if (shift == 0) {
        return;
    }
    // block[this->end_free + 1 + shift::end] = block[this->end_free + 1 + shift::start];

    char* temp[start - (this->end_free+1)];
    memcpy(temp, this->address(this->end_free + 1), start);
    memcpy(this->address(this->end_free+1+shift), temp, end);

    RecordIDs* record_ids = this->ids();
    for (u16 record_id : *record_ids) {
        u16 size, loc;
        get_header(size, loc, record_id);
        if (loc <= start) {
            loc = loc + shift;
            put_header(record_id, size, loc);
        }
    }
    this->end_free += shift;
    put_header();
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}


//Heap File
// open, close, delete, get, get_new, put, block_ids

void HeapFile::create(void) {
    this->db_open(DB_CREATE | DB_EXCL);
    SlottedPage* block = this->get_new();
    this->put(block);
}

void HeapFile::drop(void) {
    close();
    Db db(_DB_ENV, 0);
    db.remove(this->dbfilename.c_str(), nullptr, 0);
}

void HeapFile::open(void) {
    this->db_open();
}

void HeapFile::close(void) {
    this->db.close(0);
    this->closed = true;
}


// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage* HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

SlottedPage* HeapFile::get(BlockID block_id) {
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    Dbt key(&block_id, sizeof(block_id));

    SlottedPage* page = new SlottedPage(data, block_id, false);
    this->db.get(nullptr, &key, &data, 0);

    return page;
}

void HeapFile::put(DbBlock *block) {
    BlockID block_id = block->get_block_id();
    Dbt key(&block_id,sizeof(block_id));
    this->db.put(nullptr, &key, block->get_block(), 0);
}

BlockIDs* HeapFile::block_ids() {
    BlockIDs* blocks = new BlockIDs();
    for(BlockID id = 1; id < this->last; id++) {
        blocks->push_back(id);
    }
    return blocks;
}

void HeapFile::db_open(uint flags) {
    if(!this->closed) {
        return;
    }

    this->db.open(nullptr, dbfilename.c_str(), nullptr,DB_RECNO, flags, 0);
    this->closed = false;
}

//Heap Table stuff
// Heap table
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)  : DbRelation(table_name, column_names, column_attributes), file(table_name) {

}

// Create table
void HeapTable::create() {
    this->file.create();
}

// Create table if not exist
void HeapTable::create_if_not_exists() {
    try {
        this->open();
    } catch (DbException &e) {
        this->file.create();
    }
}

// Drop table
void HeapTable::drop() {
    this->file.drop();
}

// Open existing table
void HeapTable::open() {
    this->file.open();
}

// Close the table
void HeapTable::close() {
    this->file.close();
}

// Insert into table a value
Handle HeapTable::insert(const ValueDict* row) {
    open();
    return append(validate(row));
}

// Update into table
void HeapTable::update(const Handle handle, const ValueDict* new_values) {
    /*
        u16 row = this->project(handle);
        for (int i = 0; i < new - values.size(); i++) {
            row[i] = new_values[i];
        }
        u16 full_row = validate(row);
        BlockIDs* block_id = handle;
        RecordIDs* record_id = handle;
        SlottedPage* block = this->file.get(block_id);
        block->put(record_id, marshal(full_row));
        this->file.put(block);
        return handle;
    */
//fix later because it will be implemented later
}

// Delete handle from table
void  HeapTable::del(const Handle handle) {
    /*
        open();
        BlockIDs* block_id = handle;
        RecordIDs* record_id = handle;
        SlottedPage* block = this->file.get(block_id);
        block.delete(record_id);
        this->file.put(block);
    */
//fix later because it will be implemented later

}


// Select handle from table
Handles* HeapTable::select() {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id : *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id : *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;

}


// Select handle from table
Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id : *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id : *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}


ValueDict* HeapTable::project(Handle handle) {
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage* block = file.get(block_id);
    Dbt* data = block->get(record_id);
    ValueDict* row = unmarshal(data);

    delete data;
    return row;
}

ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names) {
//for later sprint
    return nullptr;
}

ValueDict* HeapTable::validate(const ValueDict* row) {
    return nullptr;
}

Handle HeapTable::append(const ValueDict* row) {
    Dbt *data = this->marshal(row);
    SlottedPage *block = this->file.get(this->file.get_last_block_id());
    RecordID record_id;

    try {
        record_id = block->add(data);
    } catch(DbBlockNoRoomError &e) {

        block = this->file.get_new();
        record_id = block->add(data);
    }
    Handle handle(file.get_last_block_id(), record_id);
    file.put(block);
    delete data;
    delete block;
    return handle;
}


// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt * HeapTable::marshal(const ValueDict * row) {
    char* bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name : this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*)(bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*)(bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char* right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt* data = new Dbt(right_size_bytes, offset);
    return data;
}


ValueDict* HeapTable::unmarshal(Dbt* data) {
//FIXME for later sprint
    return nullptr;
}