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

bool test_heap_storage() {return true;}

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
    put_header();  // ???
    put_header(this->num_records, size, this->end_free + 1);

    return this->num_records;
}

/**
 * Get a record from the block given record id
 * Return NULL if it has been deleted
 * @param record_id the id given for a record
 * @return the record for the given id
 */
Dbt* SlottedPage::get(RecordID record_id) {
    // get location and length of the record
    u16 loc, size;
    this->get_header(size, loc, record_id);

    // check if it's tombstone of deleted record
    if (loc == 0)
        return nullptr;
    
    // get data content
    char byte_string[size];
    memcpy(byte_string, this->address(loc), size);

    // Dbt(void *data, size_t size); 
    Dbt *data = new Dbt(byte_string, size);

    return data;
}

/**
 * Replace the record with the given data. Raises ValueError if it won't fit
 * @param record_id the id given for a record
 * @param data data given for record update
 * @exception DbBlockNoRoomError if no room for record update
 */ 
void SlottedPage::put(RecordID record_id, const Dbt &data) {
    // get location and size info
    u16 loc, size, new_size;
    this->get_header(size, loc, record_id);
    new_size = data.get_size();

    // check if update size 
    if (new_size > size) {

        // check if block room enough for update
        if (!this->has_room(new_size - size))
            throw DbBlockNoRoomError("block room not enough for record update");

        // slide and update data
        this->slide(loc, loc - new_size + size);
        memcpy(this->address(loc - new_size + size), data.get_data(), new_size);

    } else {
        // update data then slide
        memcpy(this->address(loc), data.get_data(), new_size);
        this->slide(loc + new_size, loc + size);
    }
    
    // update header
    this->get_header(size, loc, record_id);
    this->put_header(record_id, new_size, loc);
};

/**
 * Mark the given record_id as deleted by changing its size to 
 * zero and its location to 0. Compact the rest of the data in 
 * the block. But keep the record ids the same for everyone.
 * @param record_id the id given for a record
 */ 
void SlottedPage::del(RecordID record_id) {
    // get location and size info
    u16 loc, size;
    this->get_header(size, loc, record_id);

    // update header and slide
    this->put_header(record_id, 0, 0);
    this->slide(loc, loc + size);
};

/**
 * Get the sequence of all non-deleted record ids
 * @return ids of all non-deleted records
 */ 
RecordIDs * SlottedPage::ids(void) {return nullptr;}
// RecordIDs * SlottedPage::ids(void) {
//     u16 loc, size, i = 0;
//     RecordIDs ids[this->num_records];

//     while (i < this->num_records) {
//         this->get_header(size, loc, i + 1);
//         if (size != 0)
//             ids[i++] = i + 1;
//     }
//     return ids;
// }

/**
 * Get the size and offset for given record_id. 
 * For record_id of zero, it is the block header
 * @param &size
 * @param &loc
 * @param id
 */ 
void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id) {
    if (id == 0) {
        size = 0;
        loc = 0;
    } else {
        size = this->get_n(4 * id);
        loc = this->get_n(4 * id + 2);
    }
}

/**
 * Store the size and offset for given id
 * For id of zero, store the block header
 * @param id given id of a record
 * @param size given size to put
 * @param loc given offset to put
 */ 
void SlottedPage::put_header(RecordID id, u16 size, u16 loc) {
    // if call put_header(), use default params 
    if (id == 0) {  
        size = this->num_records;   // (0, 0, 0) ???
        loc = this->end_free;
    }
    // put header with size and offset
    put_n(4 * id, size);
    put_n(4 * id + 2, loc);
}

bool SlottedPage::has_room(u16 size) {return false;};

void SlottedPage::slide(u16 start, u16 end) {};

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

// HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
// : DbRelation(table_name, column_names, column_attributes) {};

void HeapTable::create() {}

void HeapTable::create_if_not_exists() {}

void HeapTable::drop() {}

void HeapTable::open() {}

void HeapTable::close() {}

Handle HeapTable::insert(const ValueDict *row) {return std::make_pair(1, 2);}

void HeapTable::update(const Handle handle, const ValueDict *new_values) {}

void HeapTable::del(const Handle handle) {}

Handles * HeapTable::select() {return nullptr;}

Handles * HeapTable::select(const ValueDict *where) {return nullptr;}

ValueDict * HeapTable::project(Handle handle) {return nullptr;}

ValueDict * HeapTable::project(Handle handle, const ColumnNames *column_names) {return nullptr;}

ValueDict * HeapTable::validate(const ValueDict *row) {return nullptr;}

Handle HeapTable::append(const ValueDict *row) {return std::make_pair(1, 2);}

Dbt * HeapTable::marshal(const ValueDict *row) {return nullptr;}

ValueDict * HeapTable::unmarshal(Dbt *data) {return nullptr;}