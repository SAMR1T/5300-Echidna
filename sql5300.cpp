/**
 * @file sql5300.cpp main entry for the sql5300 relational db manager for spsc5300 course
 * @author  CPSC5300-Spring2020 students
 * @version Team Echidna
 * @see Seattle U, CPSC 5300, Spring 2020
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include "SQLParser.h"

using namespace std;
using namespace hsql;

/**
 * Convert the hyrise ColumnDefinition AST back to equivalent SQL
 * @param col column definition to unparse
 * @return SQL equivalent to *col
 */
string convertColToStr(const ColumnDefinition *col) {
	string res(col->name);
	switch(col->type) {
		case ColumnDefinition::INT:
			res += " INT";
			break;
		case ColumnDefinition::DOUBLE:
			res += " DOUBLE";
			break;
		case ColumnDefinition::TEXT:
			res += " TEXT";
			break;
		default:
			res += " UNKNOWN";
			break;
	}
	return res;
}

/**
 * Convert the hyrise CreateStatement AST back to equivalent SQL
 * @param stmt statement to unparse
 * @return SQL equivalent to *stmt
 */
string executeCreate(const CreateStatement *stmt) {
	string res("CREATE TABLE ");

	if (stmt->type != CreateStatement::kTable)
		return res + "...";
	if (stmt->ifNotExists)
		res += "IF NOT EXISTS ";
	
	res += string(stmt->tableName) + " (";

	// locate the column info in the statement
	vector<ColumnDefinition*> *colms = stmt->columns;

	// get the name & type of each column
	for (uint i = 0; i < colms->size(); ++i) {
		res += convertColToStr(colms->at(i)) + ", ";
	}
	res.resize(res.size() - 2);
	res += ")";

	return res;
}

/**
 * Convert the hyrise expression item AST back to equivalent SQL
 * @param item item to unparse
 * @return SQL equivalent to *item
 */
string convertToStr(const Expr* item) {
	string res = "";
	switch(item->type) {
		case kExprStar:
			res += "*";
			break;	
		case kExprColumnRef:
			// just col name
			if (item->table == NULL) {
				res += string(item->name);
				break;
			}
			// table/alias name + col name
			if (item->alias != NULL) {
				res += string(item->alias) + "." + string(item->name);
			} else {
				res += string(item->table) + "." + string(item->name);
			}
			break;
		case kExprOperator:
			res += convertToStr(item->expr) + " ";
			res += item->opChar;
			res += " " +  convertToStr(item->expr2);
			break;
		case kExprLiteralFloat:
			res += to_string(item->fval);
			break;
		case kExprLiteralInt:
			res += to_string(item->ival);
			break;
		default:
			res += "Not Implemented";
			break;
	}
	return res;
}

/**
 * Convert the hyrise JoinDefinition AST back to equivalent SQL
 * @param join join to unparse
 * @return SQL equivalent to *join
 */
string convertJoin(const JoinDefinition *join) {
	switch(join->type) {
		case kJoinLeft:
			return " LEFT JOIN ";
		case kJoinRight:
			return " RIGHT JOIN ";
		default:
			return " JOIN ";
	}
}

/**
 * Convert the hyrise TableRef AST back to equivalent SQL
 * @param table table to unparse
 * @return SQL equivalent to *table
 */
string convertTableRef(const TableRef *table) {
	string res = "";
	switch(table->type) {
		case kTableName:
			res += string(table->name);
			if (table->alias != NULL) {
				res += " AS " + string(table->alias);
			}
			break;
		case kTableJoin:
			// JoinDefinition* join = table->join;
			res += convertTableRef(table->join->left) + convertJoin(table->join);
			res += convertTableRef(table->join->right);
			res += " ON " + convertToStr(table->join->condition);
			break;  
		case kTableCrossProduct:
			// vector<TableRef*>* tables = table->list;
			for (TableRef *tab : *table->list) {
				res += convertTableRef(tab) + ", ";
			}
			res.resize(res.size() - 2);
			break;
		default:
			res += "Not Implemented";
			break;
	}
	return res;
}

/**
 * Convert the hyrise SelectStatement AST back to equivalent SQL
 * @param stmt statement to unparse
 * @return SQL equivalent to *stmt
 */
string executeSelect(const SelectStatement *stmt) {
	string res("SELECT ");

	// get the name of cols in the select list
	vector<Expr*>* selectList = stmt->selectList;

	for (uint i = 0; i < selectList->size(); ++i) {
		// note: may have "*" instead of cols
		res += convertToStr(selectList->at(i)) + ", ";
	}
	res.resize(res.size() - 2);

	// get the name of table to select cols from
	TableRef* table = stmt->fromTable;

	res += " FROM " + convertTableRef(table);

	// get the where clause if exists
	if (stmt->whereClause != NULL)
		res += " WHERE " + convertToStr(stmt->whereClause);
	return res;
}

/**
 * Convert the hyrise SQLStatement AST back to equivalent SQL
 * @param stmt statement to unparse
 * @return SQL equivalent to *stmt
 */ 
string execute(const SQLStatement *stmt) {
	switch(stmt->type()) {
		case kStmtSelect:
			return executeSelect((const SelectStatement *)stmt);
		case kStmtCreate:
			return executeCreate((const CreateStatement *)stmt);
		case kStmtInsert:
			return "INSERT ..." ;
		default:
			return "Not implemented";
	}
}

/**
 * Validate and convert the SQL statements entered by user
 * @param argc number of user input
 * @param argv[] the content of user input
 */ 
int main(int argc, char* argv[]) {

	if (argc != 2) {
		cerr << "Usage: ./sql5300 dbenvpath" <<endl;
		return EXIT_FAILURE;
	}

	char* envHome = argv[1];

	// create BerkeleyDB environment
	DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
	env.open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);

	cout << "\nWelcome! Enter a SQL statement or \"quit\" to exit.\n" <<endl;
	
	// create user-input loop
	while (true) {
		cout << "SQL> ";
		string query;
		getline(cin, query);
		if (query.length() == 0) {
			continue;
		}
		// exit
		if (query == "quit") {
			break;
		}
		// parse query to a parse tree
		SQLParserResult* result = SQLParser::parseSQLString(query);

		// check if parse tree is valid
		if (result->isValid()) {
			for (uint i = 0; i < result->size(); ++i) {
				cout << "---> " << execute(result->getStatement(i)) << endl;
			}
		} else {
			cout << " X Invalid SQL statement." <<endl;
		}

		delete result;
	}

	return EXIT_SUCCESS;
}
