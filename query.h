#ifndef _QUERY_H_
#define _QUERY_H_

#include <string>
#include "Enclave_u.h"
#include "bufferManager.h"
#include "parser.h"

enum EXIT_PROCESS { PROCESS_CONTINUE, PROCESS_END };

typedef struct FieldData {
    char* field_name;
    DataType data_type;
    uint64_t data_size;
    uint8_t* data;

    FieldData(char* column_name, DataType data_type_arg, uint64_t data_size_arg, uint8_t* data_arg)
        : field_name(column_name),
          data_type(data_type_arg),
          data_size(data_size_arg),
          data(data_arg){};
} FieldData;

class QueryExecutor {
   public:
    BufferManager* buffer_manager;
    QueryExecutor();
    virtual ~QueryExecutor();
    bool queryExec(QueryNode* query_node);
    void getAllTable();

   private:
    std::vector<std::vector<std::vector<FieldData*>>> selectSecScanExec(QueryNode* query_node);
    void insertToOnlyTableExec(QueryNode* query_node);
    void createNewTable(QueryNode* query_node);
};

EXIT_PROCESS continue_or_end(QueryType qtype);
void query_prompt();

#endif