#include <cassert>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>

#include <pwd.h>
#include <unistd.h>
#define MAX_PATH FILENAME_MAX

#include "query.h"
#include "run.h"

bool PARSE_DEBUG;
bool PRODUCTION;
uint8_t TID;
bool NO_STDOUT;

/* Application entry */
int main(int argc, char* argv[]) {
    (void)(argc);
    (void)(argv);

    for (int i = 0; i < argc; i++) {
        PARSE_DEBUG |= !std::strcmp(argv[i], "--parse-debug");
        PRODUCTION |= !std::strcmp(argv[i], "--prod");
        if (!std::strcmp(argv[i], "--tid-1"))
            TID = 1;
        else if (!std::strcmp(argv[i], "--tid-2"))
            TID = 2;
        NO_STDOUT |= !std::strcmp(argv[i], "--no-stdout");
    }

    std::unique_ptr<QueryProcessRun> query_process_run = std::make_unique<QueryProcessRun>();

    // query processing start
    // if (PRODUCTION) {
    //     while (true) {
    //         std::string query;
    //         query_prompt();
    //         std::getline(std::cin, query);

    //         QUERY_PROCESS_RESULT status = query_process_run->run(query);
    //         switch (status) {
    //             case QUERY_PROCESS_RESULT::SUCCESS:
    //                 break;
    //             case QUERY_PROCESS_RESULT::DEBUG_END:
    //                 std::cout << "debug end.\n";
    //                 goto query_loop_end;
    //             case QUERY_PROCESS_RESULT::FAILED:
    //                 std::cout << "query process failed.\n";
    //             case QUERY_PROCESS_RESULT::EXIT:
    //                 goto query_loop_end;
    //         }
    //     }
    // }

    if (TID == 1) {
        // transaction_id: 1
        {
            // create
            {
                std::string query = "create table USER (id integer, name char(100));";
                std::cout << "Query " << query << std::endl;
                query_process_run->run(query);
            }
            // insert
            {
                int insert_num = 10;
                for (int i = 0; i < insert_num; ++i) {
                    std::string query = "insert into USER (id, name) values (" + std::to_string(i) +
                                        ",'nagatomi-san!" + std::to_string(i) + "');";
                    if (i == 0 || i + 1 == insert_num) {
                        std::cout << "Query " << i + 1 << ": " << query << std::endl;
                    } else if (i == 1) {
                        std::cout << "...\n";
                    }
                    query_process_run->run(query);
                }
            }
            // select
            {
                std::string query = "select (id, name) from USER;";
                std::cout << "Query " << query << std::endl;
                query_process_run->run(query);
            }
        }
    } else if (TID == 2) {
        // transaction_id: 2
        {
            // create
            {
                std::string query =
                    "create table STUDENT (id integer, name char(100), university char(50), club "
                    "char(50));";
                printf("Query %s\n", query.c_str());
                query_process_run->run(query);
            }
            // insert
            {
                int insert_num = 500;
                for (int i = 0; i < insert_num; ++i) {
                    std::string query =
                        "insert into STUDENT (id, name, university, club) values (" +
                        std::to_string(i) + ",'hamada_masahiro!" + std::to_string(i) +
                        "', 'NAIST', 'soccer" + std::to_string(i + 1) + "');";
                    if (i == 0 || i + 1 == insert_num) {
                        printf("Query %d: %s\n", i + 1, query.c_str());
                    } else if (i == 1) {
                        printf("...\n");
                    }
                    query_process_run->run(query);
                }
            }
            // select
            {
                std::string query = "select (id, name, university, club) from STUDENT;";
                printf("Query %s\n", query.c_str());
                query_process_run->run(query);
            }
        }
    }

query_loop_end:

    std::cout << "query process end.\n";

    return 0;
}
