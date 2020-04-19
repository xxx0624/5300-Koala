#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db_cxx.h"
/*
 *@file sql5300.cpp main entry for the sql5300 relational db manager.
 */
using namespace std;

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

	while(true) {
	cout << "SQL> ";
	string query;
	getline(cin,query);
	if(query.length() == 0)
	  continue;
	if(query == "quit")
	  break;

	cout << "Query: "<< query << endl;
	}
		
	
	return EXIT_SUCCESS;
}

