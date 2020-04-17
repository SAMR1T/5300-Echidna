/*
 * @file sql5300.cpp main entry for the sql5300 relational db manager for spsc5300 course
 * @author Team Echidna
 * @see Seattle U, CPSC 5300, Spring 2020
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include "SQLParser.h"

using namespace std;
using namespace hsql;

string executeCreate(const CreateStatement *stmt) {
	string ret("CREATE TABLE ");
	if (stmt->type != CreateStatement::kTable)
		return ret + "...";
	if (stmt->ifNotExists)
		ret += "IF NOT EXISTS ";
	ret += string(stmt->tableName) + "(...)";

	//FIXME: get column

	return ret;
}
string execute(const SQLStatement *stmt) {
	switch(stmt->type()) {
		case kStmtSelect:
			return "SELECT ...";
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

int main(int argc, char *argv[]) {

	if (argc != 2) {
		std::cerr << "Usage: ./sql5300 dbenvpath" << std::endl;
		return EXIT_FAILURE;
	}

	char *envHome = argv[1];

	// Create BerkeleyDB environment
	DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
	env.open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);

	std::cout << "\nWelcome! Enter a SQL statement or \"quit\" to exit.\n" << std::endl;

	// create user-input loop
	while (true) {
		std::cout << "SQL> ";
		std::string query;
		getline(std::cin, query);
		if (query.length() == 0) {
			continue;
		}
		// exit
		if (query == "quit") {
			break;
		}
		// parse query to a parse tree
		hsql::SQLParserResult* result = hsql::SQLParser::parseSQLString(query);

		// check if parse tree is valid
		if (result->isValid()) {
			std::cout << "your query was: " << query << std::endl;

			for (uint i = 0; i < result->size(); ++i) {
				cout << execute(result->getStatement(i)) << endl;
			}

			delete result;

		} else {
			std::cout << "Invalid SQL statement." << std::endl;
			delete result;
		}
	}

	return EXIT_SUCCESS;
}

