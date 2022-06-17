#include "run.h"
#include <iostream>
#include "parser.h"
#include "query.h"

extern bool PARSE_DEBUG;

QueryProcessRun::QueryProcessRun() : query_executor(new QueryExecutor()) {}

QueryProcessRun::~QueryProcessRun() { delete (query_executor); }

QUERY_PROCESS_RESULT QueryProcessRun::run(std::string query_str) {
    if (!table_load_flags) {
        query_executor->getAllTable();
        table_load_flags = true;
    }
    Parser::QueryParser query_parser = Parser::QueryParser(query_str);
    query_parser.parse();
    if (PARSE_DEBUG) {
        std::cout << "---PARSE RESULT---" << std::endl;
        query_parser.debugQuery(query_parser.queryNode);
        std::cout << "PARSE DEBUG END.\n";
        exit(EXIT_SUCCESS);
    }
    bool exit = query_executor->queryExec(query_parser.queryNode);
    if (exit) {
        return QUERY_PROCESS_RESULT::EXIT;
    } else {
        return QUERY_PROCESS_RESULT::SUCCESS;
    }
}