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
			
			// FIX-ME
			// for (uint i = 0; i < result->size(); ++i) {
			// 	hsql::printStatementInfo(result.getStatement(i))
			// }
			
		} else {
			cout << "Invalid SQL statement." << endl;
		}
	}

	return EXIT_SUCCESS;
}
