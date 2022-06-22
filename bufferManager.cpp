#include "bufferManager.h"
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <ext/stdio_filebuf.h>
#include <memory>
#include <random>
#include <typeinfo>
#include <vector>
#include "util.h"

extern bool PRODUCTION;

const uint16_t UINT16_BYTE_SIZE = 2;
std::string PROJECT_PATH        = "/home/masashi/workspace/db/untrust-dbms/";
const char* SCHEMA_FILE_NAME    = "SCHEMA";

inline bool BufferTag::operator==(const BufferTag& rhs) const {
    const BufferTag& lhs = *this;
    return lhs.db_node == rhs.db_node && lhs.rel_node == rhs.rel_node && lhs.fd == rhs.fd &&
           lhs.heap_file_block_id == rhs.heap_file_block_id;
}

inline bool BufferTag::operator!=(const BufferTag& rhs) const { return !(*this == rhs); }

inline std::size_t BufferTag::Hash::operator()(const BufferTag& key) const {
    std::size_t h1 = std::hash<Oid>()(key.db_node);
    std::size_t h2 = std::hash<Oid>()(key.rel_node);
    std::size_t h3 = std::hash<int>()(key.fd);
    std::size_t h4 = std::hash<uint64_t>()(key.heap_file_block_id);
    return h1 ^ h2 ^ h3 ^ h4;
}

BufferManager::BufferManager()
    : data_entry_hash({}),
      buffer_table(new BufferTable()),
      buffer_descriptor({}),
      buffer_pool({}) {}

BufferManager::~BufferManager() {
    for (auto&& descriptor : buffer_descriptor) {
        if (descriptor.flags == PageFlags::DIRTY) {
            pageFlush((uint16_t)descriptor.buffer_id.id);
        }
    }
    delete (buffer_table);
}

void BufferManager::createDataFile(const char* table_name) {
    std::ifstream ifs((PROJECT_PATH + table_name).c_str(), std::ios::binary | std::ios::in);
    if (ifs.good()) {
        return;
    }
    std::ofstream ofs((PROJECT_PATH + table_name).c_str(), std::ios::binary | std::ios::out);
    if (!ofs.good()) {
        debug_error("Failed to create data file.\n");
    }
}

BufferId BufferManager::getDataEntry(BufferTag& buffer_tag) {
    if (data_entry_hash.contains(buffer_tag)) {
        return BufferId{data_entry_hash[buffer_tag]};
    }

    // insert Tag and slot_id to buffer table
    uint32_t target_tag_slot_id = buffer_table->next_slot_id;
    BufferId buffer_id          = setNewBufferDescripter(buffer_tag);
    auto new_data_entry = std::make_shared<DataEntry>(DataEntry{buffer_tag, buffer_id, nullptr});
    if (buffer_table->bucket_slots[target_tag_slot_id] == nullptr) {
        buffer_table->bucket_slots[target_tag_slot_id] = new_data_entry;
    } else {
        auto data_entry_ptr = buffer_table->bucket_slots[target_tag_slot_id];
        for (; data_entry_ptr->data_entry != nullptr;) {
            data_entry_ptr = data_entry_ptr->data_entry;
        }
        auto last_data_entry        = data_entry_ptr;
        last_data_entry->data_entry = new_data_entry;
    }

    // insert Tag and slot_id to hash entry
    auto target =
        data_entry_hash.insert(std::pair<BufferTag, uint32_t>{buffer_tag, target_tag_slot_id});

    if (!target.second) {
        debug_error("new DataEntry failed.\n");
    }
    buffer_table->next_slot_id = (buffer_table->next_slot_id + 1) % BUCKET_SLOT_SIZE;

    return buffer_id;
}

BufferId BufferManager::setNewBufferDescripter(BufferTag& buffer_tag) {
    struct TargetBufferDescriptor {
       private:
        uint16_t target_buffer_id   = -1;
        uint64_t target_usage_count = __LONG_MAX__;

       public:
        void set(uint16_t id, uint64_t usage_count) {
            target_buffer_id   = id;
            target_usage_count = usage_count;
        }
        inline uint16_t read_id() { return target_buffer_id; }
        inline uint64_t read_usage_count() { return target_usage_count; }
    } victim_buffer_descriptor;

    // find victim page
    for (uint16_t i = 0; i < PAGE_NUMS; i++) {
        if (buffer_descriptor[i].ref_count == 0) {
            victim_buffer_descriptor.set(i, 1);
            break;
        }
        if (buffer_descriptor[i].ref_count == 0 &&
            buffer_descriptor[i].usage_count < victim_buffer_descriptor.read_usage_count()) {
            victim_buffer_descriptor.set(i, buffer_descriptor[i].usage_count);
        }
    }
    assert(victim_buffer_descriptor.read_id() > -1);

    // if victim descriptor is dirty, need to page flush.
    if (buffer_descriptor[victim_buffer_descriptor.read_id()].flags == PageFlags::DIRTY) {
        pageFlush(victim_buffer_descriptor.read_id());
    }

    BufferId buffer_id                     = BufferId{victim_buffer_descriptor.read_id()};
    BufferDescriptor new_buffer_descriptor = BufferDescriptor{
        buffer_tag, PageFlags::VALID, 1, 1, victim_buffer_descriptor.read_id(),
    };
    memcpy(&buffer_descriptor[victim_buffer_descriptor.read_id()], &new_buffer_descriptor,
           sizeof(BufferDescriptor));

    setPageToBufferPool(buffer_tag, &buffer_id);

    return buffer_id;
}

std::pair<Oid, Oid> BufferManager::getTableOid(const char* table_name) {
    if (buffer_table_info.contains(table_name)) {
        TableInfoHeader* table_info_header = buffer_table_info[table_name];
        return std::make_pair(table_info_header->db_node, table_info_header->rel_node);
    }

    debug_error("cannot reach this line at getTableOid.\n");
    return std::make_pair(__LONG_LONG_MAX__, __LONG_LONG_MAX__);
}

void BufferManager::setPageToBufferPool(BufferTag& buffer_tag, BufferId* buffer_id) {
    auto table_size = getTablePageSize(buffer_tag.table_ident);
    // need new page
    if (table_size <= buffer_tag.heap_file_block_id * PAGE_TABLE_SIZE) {
        // set default HeapHeaderInfo
        buffer_pool[buffer_id->id].heap_header_info.pd_lsn      = 0;
        buffer_pool[buffer_id->id].heap_header_info.pd_checksum = 0;
        buffer_pool[buffer_id->id].heap_header_info.pd_lower    = sizeof(HeapHeaderInfo);
        buffer_pool[buffer_id->id].heap_header_info.pd_upper    = PAGE_TABLE_SIZE;
        buffer_pool[buffer_id->id].heap_header_info.pd_special  = 0;
        buffer_descriptor[buffer_id->id].flags                  = PageFlags::DIRTY;
        return;
    } else {
        std::ifstream ifs((PROJECT_PATH + buffer_tag.table_ident),
                          std::ios::binary | std::ios::in | std::ios::out);
        if (!ifs.good()) {
            debug_error("Failed to open the file at setPageToBufferPool.\n");
        }
        ifs.seekg(buffer_tag.heap_file_block_id * PAGE_TABLE_SIZE, std::ios::beg);
        ifs.read(reinterpret_cast<char*>(&buffer_pool[buffer_id->id]), PAGE_TABLE_SIZE);
        if (ifs.fail()) {
            debug_error("Failed to read the file at setpageToBufferPool.\n");
        }
    }
}

void BufferManager::pageFlush(uint16_t buffer_id) {
    if (buffer_descriptor[buffer_id].flags != PageFlags::DIRTY) {
        printf("Try to flush non dirty page.\n");
        exit(EXIT_FAILURE);
    }

    BufferTag target_tag = buffer_descriptor[buffer_id].tag;

    std::string table_name = std::string(target_tag.table_ident);
    std::ofstream ofs((PROJECT_PATH + table_name).c_str(),
                      std::ios::binary | std::ios::out | std::ios::in);

    if (!ofs.good()) {
        debug_error("failed to open file at pageFlush.\n");
    }

    ofs.seekp(target_tag.heap_file_block_id * PAGE_TABLE_SIZE, std::ios::beg);
    ofs.write(reinterpret_cast<const char*>(&buffer_pool[buffer_id]), PAGE_TABLE_SIZE);

    if (ofs.fail()) {
        debug_error("seeking error at pageFlush.\n");
    }

    ofs.close();

    buffer_descriptor[buffer_id].flags = PageFlags::VALID;
}

void BufferManager::tablePageFlush() {
    assert(table_info_flags == PageFlags::DIRTY);
    std::ofstream ofs(PROJECT_PATH + std::string(SCHEMA_FILE_NAME),
                      std::ios::binary | std::ios::out | std::ios::in);

    if (!ofs.good()) {
        debug_error("failed to open schema file at tablePageFlush.\n");
    }

    ofs.write(reinterpret_cast<const char*>(&schema_info), sizeof(SchemaInfo));

    if (ofs.fail()) {
        debug_error("failed to write to schema file.\n");
    }

    ofs.close();

    table_info_flags = PageFlags::VALID;
}

uint64_t BufferManager::getTablePageNum(const char* table_name) {
    uint64_t page_num       = 0;
    std::string table_ident = std::string(table_name);
    FILE* fp                = fopen((PROJECT_PATH + table_ident).c_str(), "rb");

    if (fp == NULL) {
        std::cout << table_name << std::endl;
        debug_error("cannot open table file at getTablePageNum.\n");
    }

    if (fseek(fp, 0L, SEEK_END) == 0) {
        fpos_t pos;

        if (fgetpos(fp, &pos) == 0) {
            fclose(fp);
            page_num = (uint64_t)std::max((long)pos.__pos - 1, (long)0) / PAGE_TABLE_SIZE;
        }
    }

    for (const auto& [buffer_tag, _] : data_entry_hash) {
        if (!strcmp(buffer_tag.table_ident, table_name)) {
            page_num = (uint64_t)std::max(page_num, (uint64_t)(buffer_tag.heap_file_block_id + 1));
        }
    }
    return page_num;
}

uint64_t BufferManager::getTablePageSize(const char* table_name) {
    std::string table_ident = std::string(table_name);
    FILE* fp                = fopen((PROJECT_PATH + table_ident).c_str(), "rb");

    if (fp == NULL) {
        std::cout << table_name << std::endl;
        debug_error("cannot open table file at getTablePageNum.\n");
    }

    if (fseek(fp, 0L, SEEK_END) == 0) {
        fpos_t pos;

        if (fgetpos(fp, &pos) == 0) {
            fclose(fp);
            return (uint64_t)pos.__pos;
        }
    }

    fclose(fp);
    debug_error("getTablePageSize error.\n");
    return __LONG_LONG_MAX__;
}

const std::vector<
    std::vector<std::pair<std::shared_ptr<ColumnTuple>, std::pair<uint8_t*, uint16_t>>>>
BufferManager::getPageAllTupleUserData(const char* table_name, IdentList* column_ident_list,
                                       PageId page_id) {
    auto table_oid          = getTableOid(table_name);
    Oid db_node             = table_oid.first;
    Oid rel_node            = table_oid.second;
    BufferTag buffer_tag    = BufferTag{db_node, rel_node, 0, page_id, table_name};
    BufferId buffer_id      = getDataEntry(buffer_tag);
    uint8_t* page_start_ptr = (uint8_t*)&buffer_pool[buffer_id.id];
    uint16_t pd_lower       = buffer_pool[buffer_id.id].heap_header_info.pd_lower;

    // select target column
    auto table_info_header = buffer_table_info[table_name];
    assert(NULL != table_info_header);
    auto column_tuple_list = column_list_map[table_info_header->rel_node];
    assert(column_tuple_list.size() > 0);
    std::unordered_map<uint32_t, std::shared_ptr<ColumnTuple>> column_id_map;
    for (auto target_ident = column_ident_list; target_ident != NULL;
         target_ident      = target_ident->next) {
        for (uint32_t i = 0; i < column_tuple_list.size(); i++) {
            if (!strcmp(target_ident->ident, column_tuple_list[i]->column_ident)) {
                column_id_map[i + 1] = column_tuple_list[i];
            }
        }
    }
    std::vector<std::vector<std::pair<std::shared_ptr<ColumnTuple>, std::pair<uint8_t*, uint16_t>>>>
        page_all_tuple_user_data = {};
    // operate every tuple
    for (uint16_t* line_pos_ptr = (uint16_t*)(page_start_ptr + sizeof(HeapHeaderInfo));
         (uint8_t*)line_pos_ptr - page_start_ptr != pd_lower; ++line_pos_ptr) {
        page_all_tuple_user_data.push_back({});
        uint8_t* tuple_ptr     = page_start_ptr + *line_pos_ptr;
        uint8_t* tuple_end_ptr = (uint16_t*)(page_start_ptr + sizeof(HeapHeaderInfo)) < line_pos_ptr
                                     ? page_start_ptr + *(line_pos_ptr - 1)
                                     : page_start_ptr + PAGE_TABLE_SIZE;
        uint16_t field_data_num = *(uint16_t*)(tuple_ptr);
        // operate every user_data in tuple, and collect target column data
        for (int field_id = 1; field_id <= field_data_num; ++field_id) {
            if (NULL == column_id_map[field_id]) {
                continue;
            }
            uint16_t field_start_pos = *((uint16_t*)tuple_ptr + field_id);
            uint16_t field_end_pos   = field_id < field_data_num
                                         ? *((uint16_t*)tuple_ptr + field_id + 1)
                                         : (uint16_t)(tuple_end_ptr - tuple_ptr);
            uint16_t field_data_size   = field_end_pos - field_start_pos;
            uint8_t* target_field_data = (uint8_t*)calloc(1, field_data_size + 1);
            memcpy(target_field_data, tuple_ptr + field_start_pos, field_data_size);
            page_all_tuple_user_data.back().push_back(std::make_pair(
                column_id_map[field_id], std::make_pair(target_field_data, field_data_size)));
        }
        if ((uint8_t*)line_pos_ptr - page_start_ptr > pd_lower) {
            debug_error("line_pos > pd_lower error.\n");
        }
    }

    return page_all_tuple_user_data;
}

void BufferManager::insertOneTupleToOnlyTable(ValueList* value_list, const char* table_name) {
    if (!PRODUCTION) BufferManager::createDataFile(table_name);
    auto table_oid = BufferManager::getTableOid(table_name);  // ->first: db_oid, ->second: rel_oid
    uint64_t last_page_id = getTablePageNum(table_name);
    // Prevent bugs when the pagenum is 0
    if (last_page_id > 0) --last_page_id;

    BufferTag buffer_tag =
        BufferTag{table_oid.first, table_oid.second, 0, last_page_id, table_name};
    BufferId buffer_id = getDataEntry(buffer_tag);

    uint16_t pd_lower = buffer_pool[buffer_id.id].heap_header_info.pd_lower;
    uint16_t pd_upper = buffer_pool[buffer_id.id].heap_header_info.pd_upper;
    uint8_t* page_ptr = (uint8_t*)&buffer_pool[buffer_id.id];

    // get tuple size
    uint16_t field_num      = 0;
    uint16_t all_field_size = 0;
    for (ValueList* value = value_list; value != NULL; value = value->next) {
        all_field_size += value->data_size;
        ++field_num;
    }
    uint16_t tuple_size =
        (uint16_t)(UINT16_BYTE_SIZE + UINT16_BYTE_SIZE * field_num + all_field_size);

    // need new page
    if (pd_upper - pd_lower < tuple_size + UINT16_BYTE_SIZE) {
        buffer_tag = BufferTag{table_oid.first, table_oid.second, 0, last_page_id + 1, table_name};
        buffer_id  = getDataEntry(buffer_tag);
        pd_lower   = buffer_pool[buffer_id.id].heap_header_info.pd_lower;
        pd_upper   = buffer_pool[buffer_id.id].heap_header_info.pd_upper;
        page_ptr   = (uint8_t*)&buffer_pool[buffer_id.id];
    }

    // insert tuple
    uint16_t target_tuple_pos = pd_upper - tuple_size;
    assert(sizeof(HeapHeaderInfo) < target_tuple_pos && target_tuple_pos < pd_upper);
    // tuple_ptr is start pointer to new tuple
    uint8_t* tuple_ptr      = page_ptr + target_tuple_pos;
    ValueList* target_value = value_list;
    // field_num copy
    memcpy((uint16_t*)tuple_ptr, &field_num, sizeof(uint16_t));
    uint16_t* field_pos_ptr = (uint16_t*)tuple_ptr + 1;
    uint8_t* field_data_ptr = (uint8_t*)((uint16_t*)tuple_ptr + 1 + field_num);
    // copy every value to buffer_pool
    for (; target_value != NULL; target_value = target_value->next) {
        memcpy(field_data_ptr, target_value->binary_data, target_value->data_size);
        uint16_t field_pos = (uint16_t)(field_data_ptr - tuple_ptr);
        memcpy(field_pos_ptr, &field_pos, sizeof(uint16_t));
        // increment
        ++field_pos_ptr;
        field_data_ptr += target_value->data_size;
    }
    assert(field_data_ptr == page_ptr + pd_upper);

    // insert tuple line_pos
    uint16_t* target_line_pos_ptr = (uint16_t*)(page_ptr + pd_lower);
    memcpy(target_line_pos_ptr, &target_tuple_pos, sizeof(uint16_t));

    // update pd_lower and pd_upper
    buffer_pool[buffer_id.id].heap_header_info.pd_lower += sizeof(uint16_t);
    buffer_pool[buffer_id.id].heap_header_info.pd_upper -= tuple_size;

    // set a dirty flag
    buffer_descriptor[buffer_id.id].flags = PageFlags::DIRTY;
}

void BufferManager::addNewTableToBuffer(const char* table_name, IdentList* ident_list) {
    if (!PRODUCTION) BufferManager::createDataFile(SCHEMA_FILE_NAME);
    // generate random number for RelNode;
    std::random_device rd;
    std::mt19937_64 e2(rd());
    std::uniform_int_distribution<uint64_t> dist(1, std::numeric_limits<uint64_t>::max());

    TableInfoHeader* table_info_header = new TableInfoHeader();
    table_info_header->db_node         = DB_NODE;
    table_info_header->rel_node        = dist(e2);
    memcpy(table_info_header->table_name, table_name, strlen(table_name));
    std::vector<std::shared_ptr<ColumnTuple>> column_tuple_list;
    std::vector<uint16_t> column_tuple_size_list;
    // convert ident list to column list
    {
        uint16_t column_num      = 0;
        uint16_t table_info_size = 0;
        table_info_size += sizeof(TableInfoHeader);
        for (IdentList* tail_ident = ident_list; tail_ident != NULL;
             tail_ident            = tail_ident->next) {
            std::shared_ptr<ColumnTuple> column_tuple = std::make_shared<ColumnTuple>(
                static_cast<uint16_t>(strlen(tail_ident->ident)), tail_ident->ident,
                tail_ident->data_type, tail_ident->type_size, tail_ident->ident_attribute);

            column_tuple_list.push_back(column_tuple);
            column_tuple_size_list.push_back(
                (uint16_t)(sizeof(uint16_t) + sizeof(char) * column_tuple->column_ident_len +
                           sizeof(uint8_t) + sizeof(uint16_t) + sizeof(uint8_t)));
            table_info_size += sizeof(uint16_t);  // for pos of tuple
            table_info_size += column_tuple_size_list.back();
            ++column_num;
        }
        table_info_header->column_num      = column_num;
        table_info_header->table_info_size = table_info_size;
    }

    // copy column tuple to schema page
    {
        uint8_t* page_ptr      = (uint8_t*)&schema_info;
        uint8_t* new_table_ptr = page_ptr + schema_info.schema_info_header.pd_lower;

        memcpy(new_table_ptr, table_info_header, sizeof(TableInfoHeader));

        uint8_t* cur_column_pos_ptr      = new_table_ptr + sizeof(TableInfoHeader);
        uint8_t* cur_column_tuple_ptr    = new_table_ptr + table_info_header->table_info_size;
        auto column_tuple_size_list_iter = column_tuple_size_list.begin();
        uint16_t end_pos                 = table_info_header->table_info_size;
        // copy column_tuple list to schema_info content
        for (auto&& column_tuple : column_tuple_list) {
            *(uint16_t*)cur_column_pos_ptr =
                static_cast<uint16_t>(end_pos - *column_tuple_size_list_iter);
            cur_column_tuple_ptr -= *column_tuple_size_list_iter;
            *(uint16_t*)cur_column_tuple_ptr = column_tuple->column_ident_len;
            cur_column_tuple_ptr += sizeof(uint16_t);
            memcpy(cur_column_tuple_ptr, column_tuple->column_ident,
                   column_tuple->column_ident_len);
            cur_column_tuple_ptr += column_tuple->column_ident_len;
            *(uint8_t*)cur_column_tuple_ptr = static_cast<uint8_t>(column_tuple->type);
            cur_column_tuple_ptr += sizeof(uint8_t);
            *(uint16_t*)cur_column_tuple_ptr = column_tuple->type_size;
            cur_column_tuple_ptr += sizeof(uint16_t);
            *(uint8_t*)cur_column_tuple_ptr = static_cast<uint8_t>(column_tuple->attribute);
            assert(end_pos ==
                   static_cast<uint16_t>((cur_column_tuple_ptr + sizeof(uint8_t)) - new_table_ptr));
            // increment
            end_pos -= *column_tuple_size_list_iter;
            cur_column_pos_ptr += sizeof(uint16_t);
            cur_column_tuple_ptr = new_table_ptr + end_pos;
            column_tuple_size_list_iter++;
        }
        assert(new_table_ptr + end_pos == cur_column_pos_ptr);
    }

    column_list_map[table_info_header->rel_node]     = column_tuple_list;
    buffer_table_info[table_info_header->table_name] = table_info_header;
    schema_info.schema_info_header.table_num++;
    schema_info.schema_info_header.pd_lower += table_info_header->table_info_size;

    table_info_flags = PageFlags::DIRTY;

    // In current design, there is no problem for performance even if flush every new table.
    tablePageFlush();
}

void BufferManager::getAllTableToCache() {
    // open schema file
    std::ifstream ifs(PROJECT_PATH + std::string(SCHEMA_FILE_NAME),
                      std::ios::binary | std::ios::in);
    if (ifs.is_open()) {
        ifs.read(reinterpret_cast<char*>(&schema_info), SCHEMA_FILE_SIZE);
        if (ifs.fail()) {
            debug_error("Failed to read SCHEMA at getTableOid.\n");
        }
        ifs.close();
    } else {
        // there are no table yet.
        ifs.close();
        std::ofstream ofs(PROJECT_PATH + std::string(SCHEMA_FILE_NAME),
                          std::ios::binary | std::ios::out);
        char buf[SCHEMA_FILE_SIZE] = {0};
        ofs.write(buf, SCHEMA_FILE_SIZE);
        if (ofs.fail()) {
            debug_error("Failed to initialize the SCHEMA.\n");
        }
        ofs.close();
        std::ifstream ifs_2(PROJECT_PATH + std::string(SCHEMA_FILE_NAME),
                            std::ios::binary | std::ios::in);
        ifs_2.read(reinterpret_cast<char*>(&schema_info), SCHEMA_FILE_SIZE);
        if (ifs_2.fail()) {
            debug_error("Failed to read SCHEMA at getTableOid.\n");
        }
        ifs_2.close();
        schema_info.schema_info_header.table_num = 0;
        schema_info.schema_info_header.pd_lower  = sizeof(SchemaInfoHeader);
        table_info_flags                         = PageFlags::VALID;
        return;
    }

    // read every table information
    {
        uint8_t* page_ptr         = (uint8_t*)&schema_info;
        uint64_t table_num        = schema_info.schema_info_header.table_num;
        uint8_t* table_start_ptr  = page_ptr + sizeof(SchemaInfoHeader);
        uint8_t* target_table_ptr = table_start_ptr;
        for (uint64_t i = 0; i < table_num; i++) {
            TableInfoHeader* table_info_header = new TableInfoHeader();
            memcpy((uint8_t*)table_info_header, target_table_ptr, sizeof(TableInfoHeader));

            std::vector<std::shared_ptr<ColumnTuple>> column_tuple_list;
            // read all column of target table
            uint8_t* column_start_ptr = target_table_ptr + sizeof(TableInfoHeader);
            for (uint16_t j = 0; j < table_info_header->column_num; j++) {
                uint16_t target_column_pos                = *((uint16_t*)column_start_ptr + j);
                uint8_t* target_column_ptr                = target_table_ptr + target_column_pos;
                uint8_t* cur_ptr                          = target_column_ptr;
                std::shared_ptr<ColumnTuple> column_tuple = std::make_shared<ColumnTuple>();
                column_tuple->column_ident_len            = *(uint16_t*)cur_ptr;
                cur_ptr += sizeof(uint16_t);
                column_tuple->column_ident =
                    (char*)calloc(1, sizeof(column_tuple->column_ident_len));
                memcpy(column_tuple->column_ident, cur_ptr, column_tuple->column_ident_len);
                cur_ptr += column_tuple->column_ident_len;
                column_tuple->type = static_cast<DataType>(*(uint8_t*)(cur_ptr));
                cur_ptr += sizeof(uint8_t);
                column_tuple->type_size = *(uint16_t*)(cur_ptr);
                cur_ptr += sizeof(uint16_t);
                column_tuple->attribute = static_cast<ColumnAttribute>(*(uint8_t*)cur_ptr);
                column_tuple_list.push_back(column_tuple);
            }

            column_list_map[table_info_header->rel_node]     = column_tuple_list;
            buffer_table_info[table_info_header->table_name] = table_info_header;
        }
    }
    table_info_flags = PageFlags::VALID;
}
