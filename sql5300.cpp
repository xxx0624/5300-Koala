#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
#include "SQLParser.h"

/*Milestone 1
 * Koala 4/18/2020
 */
using namespace std;
using namespace hsql;

string executeCreate(const CreateStatement *stmt){
	string ret("CREATE TABLE ");
	if(stmt->type != CreateStatement::kTable)
		return ret + "...";
	if(stmt->ifNotExists)
		ret +="IF NOT EXISTS ";
	ret += string(stmt->tableName) + "(...)";
	//FIXME get columns
	return ret; 	
}

//String execute for SQL statements
string execute(const SQLStatement* stmt){
	switch (stmt->type()){
	case kStmtSelect:
		return "SELECT ...";
	case kStmtCreate:
		return executeCreate((const CreateStatement *)stmt);
	default:
		return "Not implemented.";
	}
}
int main(int argc, char *argv[]) {

	if(argc != 2){
	  cerr << "Usage: ./sql5300 dbenvpath" << endl;
	  return EXIT_FAILURE;
	}
	char *envHome = argv[1];
	
	//Berk Db stuff
	DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
	env.open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);

	try {
		env.open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);
	}
	catch (DbException & exc) {
		cerr << "(sql5300: " << exc.what() << ")" << endl;
		return 1; 
	}

	while(true) {
	cout << "SQL> ";
	string query;
	getline(cin,query);

	if(query.length() == 0)
	  continue;
	if(query == "quit")
	  break;
	
	SQLParserResult* result = SQLParser::parseSQLString(query);
	if(!result->isValid()){
		cout << "Invalid SQL: " << query << endl; 
		delete result; 
		continue;
	} else {
		cout << "Valid" << endl; 
	}
	
	for(int i = 0; i < result->size(); i++){
		cout << execute(result->getStatement(i)) << endl;
	}
	delete result;
	}
		
	
	return EXIT_SUCCESS;
}












