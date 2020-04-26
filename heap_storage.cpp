/**
 * @file heap_storage.cpp - Heap Storage Engine implementation for sql5300 relational db manager
 * - SlottedPage: DbBlock
 * - HeapFile: DbFile
 * - HeapTable: DbRelation
 * @author  Yibo Sheng, Tong(Debby) Ding
 * @version Team Echidna, Sprint Verano
 * @see Seattle U, CPSC 5300, Spring 2020
 */

#include "heap_storage.h"
#include "db_cxx.h"
#include <cstring>
#include <utility>

using namespace std;

typedef u_int16_t u16;


/* ---------------------- SlottedPage ---------------------- */

/**
 * Create a new block in SlottedPage format
 * @param block page from the database
 * @param block_id id of block within DbFile
 * @param is_new if the block exists or not
 */
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new) {
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
 * @param data data for this record 
 * @return id of the new block
 * @exception DbBlockNoRoomError if no room for new record
 */
RecordID SlottedPage::add(const Dbt *data) {
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
 * @param record_id the id given for a record
 * @return the record for a given id, or nullptr if it has been deleted
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
 * Replace a record with given data
 * @param record_id id of the record to update
 * @param data data given for record update
 * @exception DbBlockNoRoomError if no room for record update
 */ 
void SlottedPage::put(RecordID record_id, const Dbt &data) {
    // get offset and size info
    u16 loc, size, new_size;
    get_header(size, loc, record_id);
    new_size = data.get_size();

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
 * @param record_id the id given for a record to delete
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
 * @param size to hold size value
 * @param loc to hold offset value
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
 * Check if room is enough to store a record with given size
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
 * that is to the left of start to the left, if start > end.
 * Also fix up any record headers whose data has slid. Assumes 
 * there is enough room if it is a left shift (end < start).
 * @param start offset start for sliding data
 * @param end offset end for sliding data
 */ 
void SlottedPage::slide(u16 start, u16 end) {
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
 * @return a 2-byte integer
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
 * @return a pointer at given offset
 */
void* SlottedPage::address(u16 offset) {
    return (void*)((char*)this->block.get_data() + offset);
}


/* ---------------------- HeapFile ---------------------- */

/**
 * Create the database file that stores blocks for this relation
 */ 
void HeapFile::create(void) {
    db_open(DB_CREATE | DB_EXCL);
    // put in the first block
    SlottedPage *block = get_new();
    put(block);
}

/**
 * delete the database file
 */
void HeapFile::drop(void) {
    close();
    Db db(_DB_ENV, 0);
    db.remove(this->dbfilename.c_str(), nullptr, 0);
}

/**
 * Open the database file
 */
void HeapFile::open(void) {
    db_open();
}

/**
 * Close the database file
 */
void HeapFile::close(void) {
    if (this->closed)
        return;
    this->db.close(0);
    this->closed = true;
}

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

/**
 * Get a block from the database file for a given block id
 * @param block_id id of the block to get
 * @return the DbBlock for the given id
 */ 
SlottedPage* HeapFile::get(BlockID block_id) {
    // read data from berkeleydb
    Dbt key(&block_id, sizeof(block_id)), data;
    this->db.get(nullptr, &key, &data, 0);

    // return a block with the data
    SlottedPage * block = new SlottedPage(data, block_id, false); 
    return block;
}

/**
 * Write a block back to the file
 * @param block the block to put
 */  
void HeapFile::put(DbBlock *block) {
    BlockID block_id = block->get_block_id();
    Dbt key(&block_id, sizeof(block_id));
    Dbt* data = block->get_block();
    this->db.put(nullptr, &key, data, 0);
}

/**
 * Get a list of ids for the blocks in file
 * @return list of block ids
 */
BlockIDs* HeapFile::block_ids() {
    BlockIDs *ids = new vector<BlockID>();
    BlockID id = 1;
    while (id < this->last + 1)
        ids->push_back(id++);
    return ids;
}

/**
 * Open BerkeleyDB
 * @param flags
 */ 
void HeapFile::db_open(uint flags) {
    if (!this->closed)
        return;
    // this->db = new Db();
    this->db.set_re_len(DbBlock::BLOCK_SZ);
    this->dbfilename = this->name + ".db";

    // mode 0664 - readable and writable by owner, readable by everyone
    this->db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, DB_CREATE, 0664);   

    // for Btree or Recno database
    DB_BTREE_STAT stat;
    this->db.stat(nullptr, &stat, DB_FAST_STAT);
    this->last = stat.bt_ndata; // the exact number of records in the database
    this->closed = false;
}


/* ---------------------- HeapTable ---------------------- */

/**
 * Construct a HeapTable
 * @param table_name given name for table
 * @param column_names given name for columns in table
 * @param column_attributes given attributes for table columns
 */ 
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) 
    : DbRelation(table_name, column_names, column_attributes),file(table_name){}

/**                                                                     
 * Create the table                                                        
 */
void HeapTable::create(){ 
    this->file.create();
}

/**                                                                     
 * Create a new file if no file exist                                                        
 */
void HeapTable::create_if_not_exists() {
    try {
        open();
    } catch (DbException &e){
        this->file.create();
    } 
}

/**                                                                     
 * Drop the table                                                       
 */
void HeapTable::drop() {
    this->file.drop();
}

/**                                                                     
 * Open the table                                                    
 */
void HeapTable::open() {
    this->file.open();
}

/**                                                                     
 * Close the table                                                        
 */
void HeapTable::close() {
    this->file.close();
}

/**                                                                     
 * Insert a row into table   
 * @param row given row to insert  
 * @return the BlockID RecordID pair                                                   
 */
Handle HeapTable::insert(const ValueDict *row) {
    this->open();
    ValueDict* map = this->validate(row);
    return this->append(map);
}

/**                                                                     
 * Update a row in the table   
 * @param handle given BlockID RecordID pair to find row
 * @param new_values given value for row update                           
 */
void HeapTable::update(const Handle handle, const ValueDict *new_values) {

}

/**                                                                     
 * Delete a row in the table                                                      
 */
void HeapTable::del(const Handle handle) {
    // this->
}

/**                                                                  
 * Get a list of handles for qualifying rows. 
 * @return the BlockID RecordID pair  
*/
Handles * HeapTable::select() {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
    }
    return handles;
}

/**                                                                  
 * Get a list of handles for qualifying rows. 
 * @param where not used
 * @return the BlockID RecordID pair  
 */
Handles* HeapTable::select(const ValueDict* where) {
    Handles* handles = new Handles();
    BlockIDs* block_ids = file.block_ids();
    for (auto const& block_id: *block_ids) {
        SlottedPage* block = file.get(block_id);
        RecordIDs* record_ids = block->ids();
        for (auto const& record_id: *record_ids)
            handles->push_back(Handle(block_id, record_id));
    }
    return handles;
}

/**                                                                
 * Get a sequence of values for handle given by column_names                                                    
 */
ValueDict * HeapTable::project(Handle handle, const ColumnNames *column_names) {
    return nullptr;
}

/**                                                                
 * Get a sequence of values for handle given by column_names                                                     
 */
ValueDict* HeapTable:: project(Handle handle) {
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage* block = file.get(block_id);
    Dbt* data = block->get(record_id);
    ValueDict* row = unmarshal(data);
    return row;
}

/**
 * Check if the given row is acceptable to insert. 
 * @param row given row to check   
 * @exception DbRelationError
 * @return the full row dictionary
 */
ValueDict* HeapTable::validate(const ValueDict *row){
    ValueDict* full_row = new ValueDict();
        for (auto const& column_name: this->column_names){
        ValueDict::const_iterator column = row->find(column_name);
        Value value;

            if (column ==row->end()) {
                throw DbRelationError("don't know how to handle NULLS, defaults, etc. yet");
            }else{
                value = column->second;
        }
            full_row->insert(make_pair(column_name, value));
    }
        return full_row;
}

/**
 * Appends a record to the file. Assumes row is fully fleshed-out.
 * @param row given row to append   
 * @return the BlockID RecordID pair  
 */
Handle HeapTable::append(const ValueDict *row) {
    Dbt *data = this->marshal(row);
    SlottedPage *block = this->file.get(this->file.get_last_block_id());
    RecordID record_id;
    try {
       record_id = block->add(data); 
    } catch (...) {
      block = this->file.get_new();
      record_id = block->add(data);
    }
    this->file.put(block);
    return Handle(this->file.get_last_block_id(), record_id);
}

/**
 * Get the bits to go into the file
 * @param row data to convert
 * @return marshalled data
 */ 
Dbt* HeapTable::marshal(const ValueDict* row) {
    char *bytes = new char[DbBlock::BLOCK_SZ]; 
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n; 		
                offset += sizeof(int32_t);                                      
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

/**
 * Reverses marshaling process
 * @param data data to convert
 * @return unmarshalled data
 */ 
ValueDict* HeapTable::unmarshal(Dbt * data) {
    ValueDict* row = new ValueDict();
        char* bytes = (char*)data->get_data();
        uint offset = 0;
        uint col_num = 0;
        Value value;

    for (auto const& column_name : this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        value.data_type = ca.get_data_type();

        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            value.n = *(int32_t*)(bytes + offset);
            offset += sizeof(int32_t);
                row->insert(make_pair(column_name,value.n));
        }else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            u16 size = *(u16*)(bytes + offset);
            offset += sizeof(u16);

            char* Size = new char[size];
            memcpy(Size, bytes + offset, size);
            Size[size] = '\0';

            value.s = string(Size);  
            offset += size;
            row->insert(make_pair(column_name,value.s));
        } else {
            throw DbRelationError("Only know how to unmarshal INT and TEXT");
        }
    }
    return row;
}


/* ---------------------- Test ---------------------- */

/**
 * Testing function for Heap Storage.
 * @return true if testing succeeded, false otherwise 
 */
bool test_heap_storage() {

    test_slotted_page();
    std::cout << "\nslotted_page ok" << std::endl;
    
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles* handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
        return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
        return false;
    table.drop();

    return true;
}

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
