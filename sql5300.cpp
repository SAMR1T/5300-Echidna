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
			res += " INT, ";
			break;
		case ColumnDefinition::DOUBLE:
			res += " DOUBLE, ";
			break;
		case ColumnDefinition::TEXT:
			res += " TEXT, ";
			break;
		default:
			res += " UNKNOWN, ";
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
	
	res += string(stmt->tableName);
	res += "(";

	// Locate the column info in the statement
	vector<ColumnDefinition*> *colms = stmt->columns;

	// Get the name & type of each column
	for (uint i = 0; i < colms->size(); ++i) {
		res += convertColToStr(colms->at(i));
	}
	// delete colms;
	res.resize(res.size() - 2);
	res += ")";

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
	return "";
}

/**
 * Convert the hyrise TableRef AST back to equivalent SQL
 * @param table table to unparse
 * @return SQL equivalent to *table
 */
string convertTableRef(const TableRef *table) {
	string res(" FROM ");
	switch(table->type) {
		case kTableName:
			res += string(table->name);
			break;
		case kTableJoin:
			JoinDefinition* join = table->join;
			string leftTab = join->left->name;
			string rightTab = join->right->name;
			string col = join->condition->expr->name;
			res += leftTab + convertJoin(join) + rightTab;
			res += " ON " + leftTab + "." + col + " = " + rightTab + "." + col;
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

	// Get the name of cols in the select list
	vector<Expr*>* selectList = stmt->selectList;

	for (uint i = 0; i < selectList->size(); ++i) {

		Expr * item = selectList->at(i);

		// if star/wildcard is selected, add "*" instead
		if (item->type == kExprStar) {
			res += "*  ";
		} else {
			res += selectList->at(i)->name;
		   	res += ", ";
		}
	}
	res.resize(res.size() - 2);

	// Get the name of table to select cols from
	TableRef* table = stmt->fromTable;

	res += convertTableRef(table);
	
	// cout << "grpby: " << stmt->groupBy << endl;
	// cout << "limit: " << stmt->limit << endl;
	// cout << "order: " << stmt->order << endl;
	// cout << "selectDistinct: " << stmt->selectDistinct << endl;
	// cout << "unionSelect: " << stmt->unionSelect << endl;
	// cout << "whereClause: " << stmt->whereClause << endl;

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
	cout << stmt->type() << endl;
	return "FIXME";
}

/**
 * Validate and convert the SQL statements entered by user
 */ 
int main(int argc, char *argv[]) {

	if (argc != 2) {
		cerr << "Usage: ./sql5300 dbenvpath" <<endl;
		return EXIT_FAILURE;
	}

	char *envHome = argv[1];

	// Create BerkeleyDB environment
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
			cout << "your query was: " << query << endl;

			for (uint i = 0; i < result->size(); ++i) {
				cout << "convert query - "
				<< execute(result->getStatement(i)) << endl;
			}

		} else {
			cout << "Invalid SQL statement." <<endl;
		}

		delete result;
	}

	return EXIT_SUCCESS;
}
