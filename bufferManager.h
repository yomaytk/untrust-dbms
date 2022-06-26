#ifndef _BUFFER_MANAGER_H_
#define _BUFFER_MANAGER_H_

#include <stdint.h>
#include <cstdlib>
#include <memory>
#include <unordered_map>
#include <vector>
#include "c_user_types.h"
#include "util.h"

typedef uint64_t Oid;
typedef uint64_t PageId;
typedef uint64_t RelNode;

const uint32_t PAGE_NUMS        = 100;
const uint64_t PAGE_TABLE_SIZE  = 8192;
const uint32_t BUCKET_SLOT_SIZE = 100;
const uint64_t DB_NODE          = 999;
const uint64_t SCHEMA_FILE_SIZE = 8192 * 2;
const uint64_t TABLE_NAME_SIZE  = 100;

/*
    page structure (untrust memory)
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
    | pd_lsn(64) | pd_checksum(64) | pd_lower(16) | pd_upper(16) | pd_special(64) | ...
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
    | pos of plain_tuple_1(64) | pos of plain_tuple_2(64) | ...
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
    ...
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
                                                    ... | plain_tuple_2(z) | plain_tuple_1(z) |
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
*/

/*
    page structure (storage)
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
    | pd_lsn(64) | pd_checksum(64) | pd_lower(16) | pd_upper(16) | pd_special(64) | ...
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
    | pointer to plain_tuple_1(64) | pointer to plain_tuple_2(64) | ...
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
    ...
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
    ...                                             ... | encrypt_tuple_2(z) | encrypt_tuple_1(z) |
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
                                                        ... | plain_tuple_2(z) | plain_tuple_1(z) |
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
*/

/*
    plain data tuple structure
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
    | field_num(16) | field_pos_1(16) | field_pos_2(16) | ... | field_data_1 | field_data_2 | ... |
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
*/

/*
    encrypt data tuple structure
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
    enc(plain_tuple_1(z), plain_tuple_2(z), ...)
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
*/

/*
    schema_info structure
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
    | SchemaInfoHeader{table_num(64), pd_lower(16)} | table_info_tuple_1(z) | ...
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
*/

/*
    table_info tuple structure
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
    | TableInfoHeader{table_info_size(16), table_name(100), db_node(64), rel_node(64),
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
      column_num(16)} | column_pos_1(16) | column_pos_2(16) | ...
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
                                                                  ... | column_2(z) | column_1(z) |
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
    ※ column_pos is based on the start pointer of table information.
*/

/*
    column tuple structure
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
    | column_ident_len(16) | column_ident(column_ident_len) | Type(8) | TypeSize(16) | Attribute(8)|
    ーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーーー
*/

typedef struct HeapHeaderInfo {
    uint64_t pd_lsn      = 0;
    uint64_t pd_checksum = 0;
    uint16_t pd_lower    = 0;
    uint16_t pd_upper    = 0;
    uint64_t pd_special  = 0;
} HeapHeaderInfo;

static const uint64_t HEAP_CONTENT_SIZE = PAGE_TABLE_SIZE - sizeof(HeapHeaderInfo);

typedef struct BufferTag {
    Oid db_node;                  // database OID
    Oid rel_node;                 // relation table OID
    int fd;                       // file descriptor
    uint64_t heap_file_block_id;  // page id in heap file (0-index)
    const char* table_ident;      // table identifier

    struct Hash;

    bool operator==(const BufferTag& rhs) const;
    bool operator!=(const BufferTag& rhs) const;
} BufferTag;

struct BufferTag::Hash {
    typedef std::size_t result_type;

    std::size_t operator()(const BufferTag& rhs) const;
};

typedef struct BufferId {
    uint64_t id;
} BufferId;

typedef struct DataEntry {
    BufferTag tag;
    BufferId buffer_id;  // hold the id of buffer_descriptor array
    std::shared_ptr<DataEntry> data_entry = nullptr;
} DataEntry;

typedef struct BufferTable {
    std::shared_ptr<DataEntry> bucket_slots[BUCKET_SLOT_SIZE] = {nullptr};
    uint32_t next_slot_id;
} BufferTable;

typedef enum PageFlags {
    DIRTY = 100,
    VALID,
    IO_IN_PROGRESS,
    INVALID,
} PageFlags;

typedef enum class ColumnAttribute : uint8_t {
    PLAIN   = 1,
    ENCRYPT = 2,
} ColumnAttribute;

typedef struct ColumnTuple {
    uint16_t column_ident_len;
    char* column_ident;
    DataType type;
    uint16_t type_size;
    ColumnAttribute attribute;

    ColumnTuple(){};
    static inline ColumnAttribute convertToColumnAttribute(IdentAttribute ident_attribute);
    ColumnTuple(uint16_t ident_len, char* ident, DataType data_type, uint16_t type_size_arg,
                IdentAttribute ident_attribute)
        : column_ident_len(ident_len),
          column_ident(ident),
          type(data_type),
          type_size(type_size_arg) {
        switch (ident_attribute) {
            case IdentAttribute::SECRET:
                attribute = ColumnAttribute::ENCRYPT;
                break;
            case IdentAttribute::NORMAL:
                attribute = ColumnAttribute::PLAIN;
                break;
            default:
                debug_error("cannot reach this line at convertToColumnAttribute.\n");
                break;
        }
    };
} ColumnTuple;

typedef struct TableInfoHeader {
    uint16_t table_info_size;
    char table_name[TABLE_NAME_SIZE];
    uint64_t db_node;
    uint64_t rel_node;
    uint16_t column_num;
} TableInfoHeader;

typedef struct SchemaInfoHeader {
    uint64_t table_num;
    uint16_t pd_lower;
} SchemaInfoHeader;

typedef struct SchemaInfo {
    SchemaInfoHeader schema_info_header;
    uint8_t schema_info_content[SCHEMA_FILE_SIZE - sizeof(TableInfoHeader)];
} SchemaInfo;

typedef struct BufferDescriptor {
    BufferTag tag;
    PageFlags flags;
    uint16_t ref_count;    // process number accesing target page
    uint64_t usage_count;  // number of using by any process
    BufferId buffer_id;
} BufferDescriptor;

typedef struct BufferPage {
    struct HeapHeaderInfo heap_header_info;
    uint8_t heap_content[HEAP_CONTENT_SIZE] = {0};
} BufferPage;

typedef struct Tid {
    uint64_t block;
    uint16_t offset;
} Tid;

class BufferManager {
   public:
    std::unordered_map<BufferTag, uint32_t, BufferTag::Hash> data_entry_hash;
    BufferTable* buffer_table;
    std::unordered_map<std::string, TableInfoHeader*> buffer_table_info;
    std::unordered_map<RelNode, std::vector<std::shared_ptr<ColumnTuple>>> column_list_map;
    SchemaInfo schema_info;
    PageFlags table_info_flags = PageFlags::INVALID;
    BufferDescriptor buffer_descriptor[PAGE_NUMS];
    BufferPage buffer_pool[PAGE_NUMS];
    BufferManager();
    virtual ~BufferManager();
    BufferId getDataEntry(BufferTag& buffer_tag);
    std::pair<Oid, Oid> getTableOid(const char* table_name);
    uint64_t getTablePageNum(const char* table_name);
    uint64_t getTablePageSize(const char* table_name);
    const uint8_t* getTuple(Tid tid);
    const std::vector<
        std::vector<std::pair<std::shared_ptr<ColumnTuple>, std::pair<uint8_t*, uint16_t>>>>
    getPageAllTupleUserData(const char* table_name, IdentList* column_ident_list, PageId page_id);
    void insertOneTupleToOnlyTable(ValueList* value_list, const char* table_name);
    static void createDataFile(const char* table_name);
    void addNewTableToBuffer(const char* table_name, IdentList* ident_list);
    void getAllTableToCache();
    void tablePageFlush();

   private:
    BufferId setNewBufferDescriptor(BufferTag& buffer_tag);
    void pageFlush(uint16_t buffer_id);
    void setPageToBufferPool(BufferTag& buffer_tag, BufferId* buffer_id);
};

#endif