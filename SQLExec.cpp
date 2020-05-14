/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Spring 2020"
 */
#include "SQLExec.h"

using namespace std;
using namespace hsql;


// define static data
Tables *SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    case ColumnAttribute::BOOLEAN:
                        out << (value.n == 0 ? "false" : "true");
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() // FIXME
{
  if (column_names != nullptr)
    delete column_names;
  if (column_attributes != nullptr)
    delete column_attributes;
  if (rows != nullptr) {
      for (auto const& row : *rows)
        delete row;
    delete rows;
  }
}

QueryResult *SQLExec::execute(const SQLStatement *statement)
{
    // FIXME: initialize _tables table, if not yet present
    if(SQLExec::tables == nullptr)
        SQLExec::tables = new Tables();

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError &e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

//used in create function
void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute) // FIXME
{
  column_name = col->name;
  if (col->type == ColumnDefinition::INT) {
    column_attribute.set_data_type(ColumnAttribute::INT);
  } else if (col->type == ColumnDefinition::TEXT) {
    column_attribute.set_data_type(ColumnAttribute::TEXT);
  } else {
    throw SQLExecError("Unrecognized data type");
  }

}

QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch (statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}

QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    return new QueryResult("create table not implemented");  // FIXME
}

QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    return new QueryResult("create index not implemented");  // FIXME
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) { //FIXME

    if (statement->type != DropStatement::kTable) {
        throw SQLExecError("Unrecognized DROP type");
    }
    
    //get the table name
    Identifier table_name =  statement->name;

    if(table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME){
        throw SQLExecError("Cannot drop a schema table");
    }

    //get the table
    DbRelation &table = SQLExec::tables->get_table(table_name);

    //remove columns
    DbRelation &columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ValueDict where;
    where["table_name"] = Value(table_name);

    Handles* handles = columns.select(&where);
    for(auto const& handle : *handles)
        columns.del(handle);
    delete handles;

    table.drop();

    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());

    return new QueryResult(std::string("dropped ") + table_name); 
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    return new QueryResult("not implemented"); // FIXME
}

QueryResult *SQLExec::show_tables() // FIXME
{
    ColumnNames *col_names = new ColumnNames();
    ColumnAttributes *col_attribs = new ColumnAttributes();

    // We need to get the column names and it's attributes
    tables->get_columns(Tables::TABLE_NAME, *col_names, *col_attribs);

    // Handles to all table entries
    Handles *handles = tables->select();

    // Create a vector to hold all the values
    ValueDicts *rows = new ValueDicts();

    // Go through the handles and add them to the ValueDict
    for(Handle handle : *handles)
    {
        ValueDict *row = tables->project(handle, col_names);
        if(row->at("table_name") != Value("_tables") && row->at("table_name") != Value("_columns")) 
            rows->push_back(row);
    }
    delete handles;

    // Use QueryResult with all results from queries (return cn, ca, rows, message)
    string message = " Succesfully got " + to_string(rows->size()) + " rows";
    return new QueryResult(col_names, col_attribs, rows, message);
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) // FIXME
{
    // i think this will be similar to show table
    // except we focus on column...
    
    ColumnNames *col_names = new ColumnNames();
    ColumnAttributes *col_attribs = new ColumnAttributes();

    tables->get_columns(Columns::TABLE_NAME, *col_names, *col_attribs);

    // Get the rows from _columns that match the table name
    ValueDict where;
    where["table_name"] = Value(statement->tableName);

    // Get the table for
    DbRelation &column_table = tables->get_table(Columns::TABLE_NAME);

    // Get all values that match the where
    Handles *handles = column_table.select(&where);

    // Add rows the the ValueDict
    ValueDicts *rows = new ValueDicts;
    for(Handle handle : *handles)
    {
        ValueDict *row = column_table.project(handle, col_names);
        rows->push_back(row);
    }
    delete handles;

    return new QueryResult(col_names, col_attribs, rows," Sucessfully got " + to_string(rows->size()) + " rows!");
}

