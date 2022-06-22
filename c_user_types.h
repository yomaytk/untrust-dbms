#ifndef _C_USER_TYPES_H_
#define _C_USER_TYPES_H_

#include <stdint.h>

typedef uint64_t PageId;

typedef enum QueryType {
    SELECT = 1,
    CREATE = 2,
    INSERT = 3,
    DELETE = 4,
    EXIT   = 5,
    ERROR  = 6
} QueryType;

typedef enum ExpType { EQUAL = 7, NUM = 8, STR = 9, IDENT = 10 } ExpType;

typedef enum DataType { INT = 11, STRING = 12, NONE = 13 } DataType;

typedef enum IdentAttribute { NORMAL = 1, SECRET = 2 } IdentAttribute;

typedef struct IdentList NormalIdentList;
typedef struct IdentList SecretIdentList;

typedef struct ValueList {
    uint8_t* binary_data;
    uint16_t data_size;
    struct ValueList* next;
} ValueList;

struct IdentList {
    char* ident;
    DataType data_type;
    uint16_t type_size;
    struct IdentList* next;
    IdentAttribute ident_attribute;
};

struct QueryNode {
    QueryType queryType;
    char* tableName;
    struct IdentList* identList;
    struct ValueList* valueList;
    struct ExpNode* whereNode;
};

typedef ValueList NormalValueList;
typedef ValueList SecretValueList;

typedef struct ExpNode {
    ExpType expType;
    char* ident;
    char* str;
    uint64_t num;
    struct ExpNode* lhs;
    struct ExpNode* rhs;
} ExpNode;

typedef struct QueryNode NormalQueryNode;
typedef struct QueryNode SecretQueryNode;

#endif