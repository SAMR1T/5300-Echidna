#include "heap_storage.h"
#include <cstring>

bool test_heap_storage() {return true;}

/* SlottedPage */

/**
 * Create a data block with SlottedPage format
 * @param &block address of page from the database that is using SlottedPage
 * @param block_id id within DbFile
 * @param is_new indicate if the block exist or not
 */
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
    // check if block exits
    if (!is_new) {
        this->get_header(this->num_records, this->end_free);
    } else {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        this->put_header();
    }
}

// SlottedPage::~SlottedPage() {}

RecordID SlottedPage::add(const Dbt *data) {return 0;}

/**
 * Get a record from the block given record id
 * Return NULL if it has been deleted
 * @param record_id the given id of a record
 * @return Dbt the record for the given id
 */
Dbt* SlottedPage::get(RecordID record_id) {
    // get location and length of the record
    u_int16_t loc, size;
    this->get_header(size, loc, record_id);

    // check if it's tombstone of deleted record
    if (loc == 0)
        return nullptr;
    
    // get data
    char byte_string[size];
    memcpy(byte_string, this->address(loc), size);

    // Dbt(void *data, size_t size); 
    Dbt *data = new Dbt(byte_string, size);

    return data;
}

void SlottedPage::put(RecordID record_id, const Dbt &data) {};

void SlottedPage::del(RecordID record_id) {};

RecordIDs * SlottedPage::ids(void) {return nullptr;};


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
