#ifndef _RUN_H_
#define _RUN_H_

#include <string>
#include "query.h"

enum class QUERY_PROCESS_RESULT { SUCCESS = 1111, FAILED, DEBUG_END, EXIT };

class QueryProcessRun {
   public:
    QueryExecutor* query_executor;
    QueryProcessRun();
    ~QueryProcessRun();
    QUERY_PROCESS_RESULT run(std::string query_str);
    bool table_load_flags = false;
};

#endif