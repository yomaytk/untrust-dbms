/*
 * Copyright (C) 2011-2021 Intel Corporation. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   * Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in
 *     the documentation and/or other materials provided with the
 *     distribution.
 *   * Neither the name of Intel Corporation nor the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <assert.h>
#include <string.h>
#include <cstdio>
#include <fstream>
#include <iostream>
#include <thread>

#include <pwd.h>
#include <unistd.h>
#define MAX_PATH FILENAME_MAX

#include <sgx_urts.h>
#include "Enclave_u.h"
#include "input.h"
#include "lib.h"
#include "query.h"
#include "run.h"

/* Global EID shared by multiple threads */
sgx_enclave_id_t global_eid = 0;
bool PARSE_DEBUG;
bool PRODUCTION;
bool INSERT_ONLY;

typedef struct _sgx_errlist_t {
    sgx_status_t err;
    const char* msg;
    const char* sug; /* Suggestion */
} sgx_errlist_t;

/* Error code returned by sgx_create_enclave */
static sgx_errlist_t sgx_errlist[] = {
    {SGX_ERROR_UNEXPECTED, "Unexpected error occurred.", NULL},
    {SGX_ERROR_INVALID_PARAMETER, "Invalid parameter.", NULL},
    {SGX_ERROR_OUT_OF_MEMORY, "Out of memory.", NULL},
    {SGX_ERROR_ENCLAVE_LOST, "Power transition occurred.",
     "Please refer to the sample \"PowerTransition\" for details."},
    {SGX_ERROR_INVALID_ENCLAVE, "Invalid enclave image.", NULL},
    {SGX_ERROR_INVALID_ENCLAVE_ID, "Invalid enclave identification.", NULL},
    {SGX_ERROR_INVALID_SIGNATURE, "Invalid enclave signature.", NULL},
    {SGX_ERROR_OUT_OF_EPC, "Out of EPC memory.", NULL},
    {SGX_ERROR_NO_DEVICE, "Invalid SGX device.",
     "Please make sure SGX module is enabled in the BIOS, and install SGX "
     "driver afterwards."},
    {SGX_ERROR_MEMORY_MAP_CONFLICT, "Memory map conflicted.", NULL},
    {SGX_ERROR_INVALID_METADATA, "Invalid enclave metadata.", NULL},
    {SGX_ERROR_DEVICE_BUSY, "SGX device was busy.", NULL},
    {SGX_ERROR_INVALID_VERSION, "Enclave version was invalid.", NULL},
    {SGX_ERROR_INVALID_ATTRIBUTE, "Enclave was not authorized.", NULL},
    {SGX_ERROR_ENCLAVE_FILE_ACCESS, "Can't open enclave file.", NULL},
};

/* Check error conditions for loading enclave */
void print_error_message(sgx_status_t ret) {
    size_t idx = 0;
    size_t ttl = sizeof sgx_errlist / sizeof sgx_errlist[0];
    for (idx = 0; idx < ttl; idx++) {
        if (ret == sgx_errlist[idx].err) {
            if (NULL != sgx_errlist[idx].sug) printf("Info: %s\n", sgx_errlist[idx].sug);
            printf("Error: %s\n", sgx_errlist[idx].msg);
            break;
        }
    }

    if (idx == ttl)
        printf(
            "Error code is 0x%X. Please refer to the \"Intel SGX SDK Developer "
            "Reference\" for more details.\n",
            ret);
}

void sgx_error_exit(sgx_enclave_id_t gid) {
    sgx_destroy_enclave(gid);
    exit(EXIT_FAILURE);
}

/* Initialize the enclave:
 *   Call sgx_create_enclave to initialize an enclave instance
 */
int initialize_enclave(void) {
    sgx_status_t ret = SGX_ERROR_UNEXPECTED;

    /* Call sgx_create_enclave to initialize an enclave instance */
    /* Debug Support: set 2nd parameter to 1 */
    ret = sgx_create_enclave(ENCLAVE_FILENAME, SGX_DEBUG_FLAG, NULL, NULL, &global_eid, NULL);
    if (ret != SGX_SUCCESS) {
        print_error_message(ret);
        return -1;
    }

    return 0;
}

/* OCall functions */
void ocall_print_string(const char* str) {
    /* Proxy/Bridge will check the length and null-terminate
     * the input string to prevent buffer overflow.
     */
    printf("%s", str);
}

void ocall_insert_sscanf(struct UserRow* user_row, struct InputBuf* input_buf) {
    sscanf(input_buf->buf, "INSERT INTO USER (id, name, belongs) VALUES (%d , %s , %s );",
           &(user_row->id), user_row->name, user_row->belongs);
}

void ocall_create_sscanf(struct Schema* schema, struct InputBuf* input_buf) {
    // schema->data_names = (char**)malloc(sizeof(char[100]) * 3);
    // schema->data_types = (DataType*)malloc(sizeof(DataType) * 3);
    char types[3][100];
    // sscanf(input_buf->buf, "CREATE TABLE %s { %d %d , %d %s , %d %s };",
    //        schema->schema_name, schema->data_names[0], types[0],
    //        schema->data_names[1], types[1], schema->data_names[2], types[2]);
    for (int i = 0; i < 3; i++) {
        if (strncmp(types[i], "INT", 3) == 0) {
            schema->data_types[i] = INT;
        } else if (strncmp(types[i], "STRING", 6)) {
            schema->data_types[i] = STRING;
        } else {
            printf("invalid data type.\n");
        }
    }
}

void* ocall_page_malloc() {}

// void close_db(EncTable* enc_table, char* filepath) { free_table(enc_table); }

void ocall_write_buf_to_file(const char* filename, const uint8_t* buf, size_t bsize, long offset) {
    if (filename == NULL || buf == NULL || bsize == 0) return;
    std::ofstream ofs(filename, std::ios::binary | std::ios::out);
    if (!ofs.good()) {
        std::cout << "Failed to open the file \"" << filename << "\"" << std::endl;
        return;
    }
    ofs.seekp(offset, std::ios::beg);
    ofs.write(reinterpret_cast<const char*>(buf), bsize);
    if (ofs.fail()) {
        std::cout << "Failed to write the file \"" << filename << "\"" << std::endl;
        return;
    }
    return;
}

void ocall_test(TableAttribute* tableAttribute) {
    // exit(0);
}

/* Application entry */
int SGX_CDECL main(int argc, char* argv[]) {
    (void)(argc);
    (void)(argv);

    if (argc > 1) {
        PARSE_DEBUG = !strcmp(argv[1], "--parse-debug");
        PRODUCTION  = !strcmp(argv[1], "--prod");
        INSERT_ONLY = !strcmp(argv[1], "--insert");
    }

    /* Initialize the enclave */
    if (PRODUCTION) {
        if (initialize_enclave() < 0) {
            printf("Enter a character before exit ...\n");
            getchar();
            return -1;
        }

        printf("Enclave successfully initialized.\n");
    }

    std::unique_ptr<InputBuf> input_buf_wrapper        = std::make_unique<InputBuf>();
    std::unique_ptr<QueryProcessRun> query_process_run = std::make_unique<QueryProcessRun>();

    // query start
    if (PRODUCTION) {
        while (true) {
            std::string query;
            query_prompt();
            std::getline(std::cin, query);

            QUERY_PROCESS_RESULT status = query_process_run->run(query);
            switch (status) {
                case QUERY_PROCESS_RESULT::SUCCESS:
                    break;
                case QUERY_PROCESS_RESULT::DEBUG_END:
                    std::cout << "debug end.\n";
                    goto query_loop_end;
                case QUERY_PROCESS_RESULT::FAILED:
                    std::cout << "query process failed.\n";
                case QUERY_PROCESS_RESULT::EXIT:
                    goto query_loop_end;
            }
        }
    }

    // insert
    // for (int i = 0; i < 500; ++i) {
    //     std::string query =
    //         "insert into USER (id, name) values (" + std::to_string(i) + ", 'nagatomi');";
    //     std::cout << "Query " << i + 1 << ": " << query << std::endl;
    //     QUERY_PROCESS_RESULT status = query_process_run->run(query);
    //     if (INSERT_ONLY) {
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

    // select
    // {
    //     std::string query = "select (id, name) from USER;";
    //     std::cout << "Query " << query << std::endl;
    //     QUERY_PROCESS_RESULT status = query_process_run->run(query);
    //     switch (status) {
    //         case QUERY_PROCESS_RESULT::SUCCESS:
    //             break;
    //         case QUERY_PROCESS_RESULT::DEBUG_END:
    //             std::cout << "debug end.\n";
    //             goto query_loop_end;
    //         case QUERY_PROCESS_RESULT::FAILED:
    //             std::cout << "query process failed.\n";
    //         case QUERY_PROCESS_RESULT::EXIT:
    //             goto query_loop_end;
    //     }
    // }

    // create
    // {
    //     std::string query = "create table USER (id integer, name char(100));";
    //     std::cout << "Query " << query << std::endl;
    //     QUERY_PROCESS_RESULT status = query_process_run->run(query);
    //     switch (status) {
    //         case QUERY_PROCESS_RESULT::SUCCESS:
    //             break;
    //         case QUERY_PROCESS_RESULT::DEBUG_END:
    //             std::cout << "debug end.\n";
    //             goto query_loop_end;
    //         case QUERY_PROCESS_RESULT::FAILED:
    //             std::cout << "query process failed.\n";
    //         case QUERY_PROCESS_RESULT::EXIT:
    //             goto query_loop_end;
    //     }
    // }

    // basic SQL transaction
    {
        // create
        {
            std::string query = "create table USER (id integer, name char(100));";
            std::cout << "Query " << query << std::endl;
            QUERY_PROCESS_RESULT status = query_process_run->run(query);
            switch (status) {
                case QUERY_PROCESS_RESULT::SUCCESS:
                    break;
                case QUERY_PROCESS_RESULT::DEBUG_END:
                    std::cout << "debug end.\n";
                    goto query_loop_end;
                case QUERY_PROCESS_RESULT::FAILED:
                    std::cout << "query process failed.\n";
                case QUERY_PROCESS_RESULT::EXIT:
                    goto query_loop_end;
            }
        }
        // insert
        {
            for (int i = 0; i < 500; ++i) {
                std::string query =
                    "insert into USER (id, name) values (" + std::to_string(i) + ",'nagatomi');";
                std::cout << "Query " << i + 1 << ": " << query << std::endl;
                QUERY_PROCESS_RESULT status = query_process_run->run(query);
                if (INSERT_ONLY) {
                    switch (status) {
                        case QUERY_PROCESS_RESULT::SUCCESS:
                            break;
                        case QUERY_PROCESS_RESULT::DEBUG_END:
                            std::cout << "debug end.\n";
                            goto query_loop_end;
                        case QUERY_PROCESS_RESULT::FAILED:
                            std::cout << "query process failed.\n";
                        case QUERY_PROCESS_RESULT::EXIT:
                            goto query_loop_end;
                    }
                }
            }
        }
        // select
        {
            std::string query = "select (id, name) from USER;";
            std::cout << "Query " << query << std::endl;
            QUERY_PROCESS_RESULT status = query_process_run->run(query);
            switch (status) {
                case QUERY_PROCESS_RESULT::SUCCESS:
                    break;
                case QUERY_PROCESS_RESULT::DEBUG_END:
                    std::cout << "debug end.\n";
                    goto query_loop_end;
                case QUERY_PROCESS_RESULT::FAILED:
                    std::cout << "query process failed.\n";
                case QUERY_PROCESS_RESULT::EXIT:
                    goto query_loop_end;
            }
        }
    }

query_loop_end:

    std::cout << "query process end.\n";

    if (PRODUCTION) {
        /* Destroy the enclave */
        sgx_status_t ret = sgx_destroy_enclave(global_eid);

        if (ret != SGX_SUCCESS) {
            printf("failed to destroy Enclave.\n");
            return SGX_ERROR_UNEXPECTED;
        }

        printf("Enclave destroyed.\n");
    }

    return 0;
}
