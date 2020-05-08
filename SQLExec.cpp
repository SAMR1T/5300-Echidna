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
ostream &operator<<(ostream &out, const QueryResult &qres) 
{
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

// FIX ME --> Not done, just an idea
QueryResult::~QueryResult()
{
    delete tables;
    delete column_names;
    delete column_attributes;
    for(ValueDict *row: *rows)
            delete row;
    delete rows;
}


QueryResult *SQLExec::execute(const SQLStatement *statement)
{
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

void SQLExec::column_definition(const ColumnDefinition *col, Identifier &column_name, ColumnAttribute &column_attribute)
{
    throw SQLExecError("not implemented");  // FIXME
}

QueryResult *SQLExec::create(const CreateStatement *statement)
{
    swtich(statement->type)
    {
        case CreateStatement::kTable:
            return create_table(statement);
        default:
            return new QueryResult("Only CREATE TABLE is supported");
    }
}

// FIX ME --> Not done, just based on his instructions but won't work?
QueryResult *SQLExec::drop(const DropStatement *statement) 
{

    if (statement->type != DropStatement::kTable)
        throw SQLExecError("Unrecognized DROP type");
    delete handles;

    remove.drop();

    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());

    return new QueryResult(std::string("dropped ") + table_name); 
}

QueryResult *SQLExec::show(const ShowStatement *statement) 
{
    switch(statement->type)
    {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        default:
            throw SQLExecError(" not implemented, only SHOW TABLES and SHOW COLUMNS is supported.");
    } 
    return NULL; 
}

QueryResult *SQLExec::show_tables()
{
    ColumnNames *resultsColNames = new ColumnNames();
    ColumnAttributes *resultsColAttribs = new ColumnAttributes();

    // We need to get the column names and it's attributes
    tables->get_columns(Tables::TABLE_NAME, *resultsColNames, *resultsColAttribs);

    // Handles to all table entries
    Handles *handles = tables->select();

    // Create a vector to hold all the values
    ValueDicts *rows = new ValueDicts();

    // Go through the handles and add them to the ValueDict
    for(Handle handle : *handles)
    {
        ValueDict *row = tables->project(handle, resultsColNames);
        if(row->at("table_name") != Value("_tables") && row->at("table_name") != Value("_columns")) 
            rows->push_back(row);
    }
    delete handles;

    // Use QueryResult with all results from queries (return cn, ca, rows, message)
    string message = " Succesfully got " + to_string(rows->size()) + " rows";
    return new QueryResult(resultsColNames, resultsColAttribs, rows, message);
}

QueryResult *SQLExec::show_columns(const ShowStatement *statement) 
{
    // i think this will be similar to show table
    // except we focus on column...
    return new QueryResult("not implemented"); // FIXME
}

