#include "heap_storage.h"
#include <cstring>

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
        this->get_header(this->num_records, this->end_free);
    
    // initiate record cnt, free end, header
    } else {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        this->put_header();
    }
}

// SlottedPage::~SlottedPage() {}

/**
 * Add a new record to the block given data
 * @param data the data for this record 
 * @return id of the new block
 * @exception DbBlockNoRoomError if no room for new record
 */
RecordID SlottedPage::add(const Dbt *data) {
    // get the size of given data
    u_int16_t size = data->get_size();

    // check if room not enough
    if (!this->has_room(size + 4))
        throw DbBlockNoRoomError("block room not enough for new record");
    
    // udpate record cnt and end of free space
    this->num_records += 1;
    this->end_free -= size;

    // add data
    memcpy(this->address(this->end_free + 1), data->get_data(), size);

    // add header
    this->put_header(this->num_records, size, this->end_free + 1);

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
    u_int16_t loc, size;
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
    u_int16_t loc, size, new_size;
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
    u_int16_t loc, size;
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
//     u_int16_t loc, size, i = 0;
//     RecordIDs ids[this->num_records];

//     while (i < this->num_records) {
//         this->get_header(size, loc, i + 1);
//         if (size != 0)
//             ids[i++] = i + 1;
//     }
//     return ids;
// }


// u_int16_t num_records;
// u_int16_t end_free;


u_int16_t SlottedPage::get_n(u_int16_t offset) { return 0;}

/**
 * Get the size and offset for given record_id. 
 * For record_id of zero, it is the block header
 * @param &size
 * @param &loc
 * @param id
 */ 
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id) {
    if (id == 0) {
        size = 0;
        loc = 0;
    } else {
        size = this->get_n(4 * id);
        loc = this->get_n(4 * id + 2);
    }
}

void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc) {

}

bool SlottedPage::has_room(u_int16_t size) {return false;};

void SlottedPage::slide(u_int16_t start, u_int16_t end) {};



void SlottedPage::put_n(u_int16_t offset, u_int16_t n) {};

void * SlottedPage::address(u_int16_t offset) {return nullptr;};
