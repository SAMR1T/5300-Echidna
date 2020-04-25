/**
 * @file heap_storage.cpp - Heap Storage Engine implementation for sql5300 relational db manager
 * SlottedPage: DbBlock
 * HeapFile: DbFile
 * HeapTable: DbRelation
 *
 * @author  CPSC5300-Spring2020 students
 * @version Team Echidna
 * @see Seattle U, CPSC 5300, Spring 2020
 */

#include "heap_storage.h"
#include "db_cxx.h"
#include <cstring>

using namespace std;

typedef u_int16_t u16;

/* SlottedPage */

/**
 * Create a new block with SlottedPage format
 * @param block page from the database that is using SlottedPage
 * @param block_id id within DbFile
 * @param is_new indicate if the block exist or not
 */
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    // check if block exits
    if (!is_new) {
        get_header(this->num_records, this->end_free);
    
    // initiate record cnt, free end, header
    } else {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    }
}

/**
 * Add a new record to the block given data
 * @param data the data for this record 
 * @return id of the new block
 * @exception DbBlockNoRoomError if no room for new record
 */
RecordID SlottedPage::add(const Dbt *data) {
    // get the size of given data
    u16 size = (u16) data->get_size();

    // check if room not enough
    if (!has_room(size + 4))
        throw DbBlockNoRoomError("block room not enough for new record");
    
    // udpate record cnt and end of free space
    this->num_records += 1;
    this->end_free -= size;

    // add data
    memcpy(this->address(this->end_free + 1), data->get_data(), size);

    // add header
    put_header();  // update header 0 (record_nums, free end)
    put_header(this->num_records, size, this->end_free + 1);

    return this->num_records;
}

/**
 * Get a record from the block given record id
 * Return nullptr if it has been deleted
 * @param record_id the id given for a record
 * @return the record for the given id
 */
Dbt* SlottedPage::get(RecordID record_id) {
    // get offset and length of the record
    u16 loc, size;
    get_header(size, loc, record_id);

    // check if it's tombstone of deleted record
    if (loc == 0)
        return nullptr;
    
    // form record to return
    Dbt *data = new Dbt(this->address(loc), size);
    return data;
}

/**
 * Replace the record with the given data. Raises ValueError if it won't fit
 * @param record_id the id given for a record
 * @param data data given for record update
 * @exception DbBlockNoRoomError if no room for record update
 */ 
void SlottedPage::put(RecordID record_id, const Dbt &data) {
    // get offset and size info
    u16 loc, size, new_size;
    get_header(size, loc, record_id);
    new_size = data.get_size();

    // check if update size 
    if (new_size > size) {

        // check if block room enough for update
        if (!has_room(new_size - size))
            throw DbBlockNoRoomError("block room not enough for record update");

        // slide and update data
        slide(loc, loc - new_size + size);
        memcpy(this->address(loc - new_size + size), data.get_data(), new_size);

    } else {
        // update data then slide
        memcpy(this->address(loc), data.get_data(), new_size);
        slide(loc + new_size, loc + size);
    }
    // update header
    get_header(size, loc, record_id);
    put_header(record_id, new_size, loc);
};

/**
 * Mark the given record_id as deleted by changing its size to 
 * zero and its offset to 0. Compact the rest of the data in 
 * the block. But keep the record ids the same for everyone.
 * @param record_id the id given for a record
 */ 
void SlottedPage::del(RecordID record_id) {
    // get offset and size info
    u16 loc, size;
    get_header(size, loc, record_id);

    // update header and slide
    put_header(record_id, 0, 0);
    slide(loc, loc + size);
};

/**
 * Get the sequence of all non-deleted record ids
 * @return ids of all non-deleted records
 */ 
RecordIDs* SlottedPage::ids(void) {
    u16 loc, size, i = 1;
    RecordIDs *ids = new vector<RecordID>();

    while (i < this->num_records + 1) {
        get_header(size, loc, i);

        // non-deleted record has size > 0
        if (size > 0)
            ids->push_back((RecordID)i);
        ++i;
    }
    return ids;
}

/**
 * Get the size and offset for given record_id
 * For record_id of zero, it is the block header
 * @param &size addr to hold size
 * @param &loc addr to hold offset
 * @param id given id of a record
 */ 
void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id) {
    size = this->get_n(4 * id);
    loc = this->get_n(4 * id + 2);
}

/**
 * Store the size and offset for given id
 * For id of zero, store the block header
 * @param id given id of a record
 * @param size given size to put
 * @param loc given offset to put
 */ 
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    // check if update header 0 (num records, free end)
    if (id == 0) {  
        size = this->num_records;  
        loc = this->end_free;
    }
    // put header with size and offset         
    put_n(4 * id, size);
    put_n(4 * id + 2, loc);
}

/**
 * Calculate if room is enough to store a record with given size
 * @param size the size of record to store
 * @return True if enough room, otherwise False
 */
bool SlottedPage::has_room(u16 size) {
    u16 available = this->end_free - (this->num_records + 2) * 4;
    return size <= available;
}

/**
 * Remove data from offset [start, end) by sliding data that 
 * is to the left of start to the right, if start < end.  
 * Make room for extra data from end to start by sliding data 
 * that is to the left of start to the left, If start > end.
 * Also fix up any record headers whose data has slid. Assumes 
 * there is enough room if it is a left shift (end < start).
 * @param start offset start for sliding data
 * @param end offset end for sliding data
 */ 
void SlottedPage::slide(u16 start, u16 end) {
    // calc shift
    u16 shift = end - start;
    if (shift == 0)
        return;

    // slide
    u16 loc = this->end_free + 1;
    memcpy(this->address(loc + shift), this->address(loc), start - loc);

    // update header
    u16 size;
    RecordIDs *ids = this->ids();
    RecordID *id = ids->data();

    for (uint i = 0; i < ids->size(); ++i) {
        get_header(size, loc, *id);
        if (loc <= start) {
            loc += shift;
            put_header(*id, size, loc);
        }
        id++;
    }

    // update free end offset
    this->end_free += shift;
    this->put_header();
};

/** 
 * Get a 2-byte integer at given offset in block
 * @param offset given offset in block
 * @return given 2-byte integer to get
 */
u16 SlottedPage::get_n(u16 offset) {
    return *(u16*)this->address(offset);
}

/** 
 * Put a 2-byte integer at given offset in block
 * @param offset given offset in block
 * @param n given 2-byte integer to put
 */
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16*)this->address(offset) = n;
}

/** 
 * Make a void* pointer for a given offset into the data block
 * @param offset given offset in block
 * @return void* pointer for given offset
 */
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}


bool test_heap_storage() {
    char block[DbBlock::BLOCK_SZ];
    Dbt dbtblock(block, DbBlock::BLOCK_SZ);
    SlottedPage page(dbtblock, 1, true);
    return true;
}

/* HeapFile */

void HeapFile::create(void) {}

void HeapFile::drop(void) {}

void HeapFile::open(void) {}

void HeapFile::close(void) {}

/**
 * Allocate a new block for the database file
 * @return new empty DbBlock managing records in this block and its block id
 */ 
SlottedPage* HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));

    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage* page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

SlottedPage * HeapFile::get(BlockID block_id) {return nullptr;}

void HeapFile::put(DbBlock *block) {}

BlockIDs * HeapFile::block_ids() {return nullptr;}

void HeapFile::db_open(uint flags) {}




/* HeapTable */


void HeapTable::create(){ 
    cout << "Creating" << endl;
    this->file.create();
}


void HeapTable::create_if_not_exists() {
    try {
      this->open();
    } catch (DbException &e){
      this->file.create();
    } 
}


void HeapTable::drop() {
    this->file.drop();
}

void HeapTable::open() {
    this->file.open();
}

void HeapTable::close() {
    this->file.close();
}

Handle HeapTable::insert(const ValueDict *row) {
    this->open();
    return this->append(this->validate(row));
}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {}

void HeapTable::del(const Handle handle) {}



Handles * HeapTable::select() {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}


Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}


ValueDict * HeapTable::project(Handle handle, const ColumnNames *column_names) {return nullptr;}

// return the bits to go into the file
// // caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
           *(int32_t*) (bytes + offset) = value.n;                                         offset += sizeof(int32_t);                                      
	} else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes+offset, value.s.c_str(), size); //assume ascii for now
	    offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

ValueDict * HeapTable::unmarshal(Dbt *data) {return nullptr;}
