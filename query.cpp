#include "query.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cassert>
#include "bufferManager.h"
#include "util.h"

EXIT_PROCESS continue_or_end(QueryType qtype) {
    switch (qtype) {
        case SELECT:
        case INSERT:
        case CREATE:
            return PROCESS_CONTINUE;
        case EXIT:
            return PROCESS_END;
        case ERROR:
            printf("invalid query.\n");
            return PROCESS_CONTINUE;
            break;
        default:
            printf("continue_or_end error.\n");
            exit(EXIT_FAILURE);
    }
}

QueryExecutor::QueryExecutor() : buffer_manager(new BufferManager()) {}

QueryExecutor::~QueryExecutor() { delete (buffer_manager); }

std::vector<std::vector<std::vector<FieldData*>>> QueryExecutor::selectSecScanExec(
    QueryNode* query_node) {
    assert(query_node->queryType == QueryType::SELECT);
    uint64_t table_page_num = buffer_manager->getTablePageNum(query_node->tableName);
    std::vector<std::vector<std::vector<FieldData*>>> all_page_data_list = {};
    // operate every page
    for (PageId i = 0; i < table_page_num; ++i) {
        auto page_all_tuple_user_data = buffer_manager->getPageAllTupleUserData(
            query_node->tableName, query_node->identList, i);
        IdentList* ident                               = query_node->identList;
        std::vector<std::vector<FieldData*>> page_data = {};
        // operate every tuple
        for (int j = 0; (__SIZE_TYPE__)j < page_all_tuple_user_data.size(); ++j) {
            auto select_tuple_user_data = page_all_tuple_user_data[j];
            // operate every user data
            std::vector<FieldData*> tuple_data = {};
            for (int k = 0; (__SIZE_TYPE__)k < select_tuple_user_data.size(); k++) {
                auto target_column_and_data = select_tuple_user_data[k];
                auto target_column          = target_column_and_data.first;
                auto target_data            = target_column_and_data.second.first;
                auto target_data_size       = target_column_and_data.second.second;
                auto field_data = new FieldData(target_column->column_ident, target_column->type,
                                                target_data_size, target_data);
                tuple_data.push_back(field_data);
            }
            page_data.push_back(tuple_data);
        }
        all_page_data_list.push_back(page_data);
    }
    for (int i = 0; (__SIZE_TYPE__)i < all_page_data_list.size(); ++i) {
        std::cout << "page: " << i << std::endl;
        auto page_data = all_page_data_list[i];
        for (int j = 0; (__SIZE_TYPE__)j < page_data.size(); ++j) {
            std::cout << "record " << j << ": ";
            auto tuple_data = page_data[j];
            for (int k = 0; (__SIZE_TYPE__)k < tuple_data.size(); ++k) {
                switch (tuple_data[k]->data_type) {
                    case DataType::INT:
                        std::cout << tuple_data[k]->field_name << ": "
                                  << *(int*)tuple_data[k]->data;
                        break;
                    case DataType::STRING:
                        std::cout << tuple_data[k]->field_name << ": " << tuple_data[k]->data;
                        break;
                    default:
                        debug_error("selectSecScanExec undefined DataType error-2.\n");
                        break;
                }
                if ((__SIZE_TYPE__)k + 1 < tuple_data.size()) std::cout << ',';
            }
            std::cout << std::endl;
        }
    }
    return all_page_data_list;
}

void QueryExecutor::insertToOnlyTableExec(QueryNode* query_node) {
    assert(query_node->queryType == QueryType::INSERT);
    buffer_manager->insertOneTupleToOnlyTable(query_node->valueList, query_node->tableName);
}

void QueryExecutor::createNewTable(QueryNode* query_node) {
    assert(query_node->queryType == QueryType::CREATE);
    buffer_manager->addNewTableToBuffer(query_node->tableName, query_node->identList);
}

void QueryExecutor::getAllTable() { buffer_manager->getAllTableToCache(); }

bool QueryExecutor::queryExec(QueryNode* query_node) {
    bool exit = false;
    switch (query_node->queryType) {
        case QueryType::SELECT:
            selectSecScanExec(query_node);
            break;
        case QueryType::INSERT:
            insertToOnlyTableExec(query_node);
            break;
        case QueryType::CREATE:
            createNewTable(query_node);
            break;
        case QueryType::EXIT:
            exit = true;
            break;
        default:
            debug_error("undefined query node.\n");
            break;
    }
    return exit;
}

void query_prompt() { printf("> "); }