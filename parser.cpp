#include "parser.h"
#include <cassert>
#include <cctype>
#include <cstring>
#include <iostream>
#include <vector>
#include "c_user_types.h"
#include "magic_enum.hpp"
#include "util.h"

extern bool PARSE_DEBUG;
static const uint64_t INTEGER_SIZE = 4;

std::array<std::tuple<std::string, Parser::TokenType>, 13> Parser::RESERVED_WORDS = {
    std::make_tuple("select", TokenType::SELECT),  std::make_tuple("from", TokenType::FROM),
    std::make_tuple("insert", TokenType::INSERT),  std::make_tuple("into", TokenType::INTO),
    std::make_tuple("values", TokenType::VALUES),  std::make_tuple("create", TokenType::CREATE),
    std::make_tuple("table", TokenType::TABLE),    std::make_tuple("delete", TokenType::DELETE),
    std::make_tuple("integer", TokenType::INT),    std::make_tuple("char", TokenType::CHAR),
    std::make_tuple("where", TokenType::WHERE),    std::make_tuple("exit", TokenType::EXIT),
    std::make_tuple("encrypt", TokenType::ENCRYPT)};

std::map<std::string, Parser::TokenType> Parser::SIGNALS = {
    {";", TokenType::SEMI},   {"*", TokenType::ALLSTAR}, {"(", TokenType::LBRACE},
    {")", TokenType::RBRACE}, {",", TokenType::COMMA},   {"=", TokenType::EQ}};

Parser::QueryParser::QueryParser(std::string program)
    : query(program),
      charIterator(0),
      tokenIterator(0),
      tokenList((std::vector<Token*>){}),
      queryNode(nullptr) {}

Parser::QueryParser::~QueryParser() {
    for (auto token : tokenList) {
        free(token);
    }
}

void Parser::QueryParser::parse() {
    if (PARSE_DEBUG) std::cout << "LEX START." << std::endl;
    // lex
    tokenList = lex();
    // parse
    if (PARSE_DEBUG) std::cout << "PARSE START." << std::endl;
    queryNode = stmParse();
    tokenTypeAssert(TokenType::SEMI);
}

Parser::Token* Parser::QueryParser::nextToken() {
    Token* token = tokenList[tokenIterator];
    tokenIterator++;
    return token;
}

Parser::Token* Parser::QueryParser::currentToken() { return tokenList[tokenIterator]; }

inline void Parser::QueryParser::tokenIteratorInc() { tokenIterator++; }

bool Parser::QueryParser::isTokenTypeInc(TokenType tokenType) {
    if (static_cast<uint64_t>(tokenList[tokenIterator]->tokenType) ==
        static_cast<uint64_t>(tokenType)) {
        tokenIterator++;
        return true;
    } else {
        return false;
    }
}

bool Parser::QueryParser::isTokenType(TokenType tokenType) {
    if (static_cast<uint64_t>(tokenList[tokenIterator]->tokenType) ==
        static_cast<uint64_t>(tokenType)) {
        return true;
    } else {
        return false;
    }
}

void Parser::QueryParser::tokenTypeAssert(TokenType tokenType) {
    if (!(static_cast<uint64_t>(tokenList[tokenIterator]->tokenType) ==
          static_cast<uint64_t>(tokenType))) {
        std::cout << "token type assert error, curent: "
                  << static_cast<uint64_t>(tokenList[tokenIterator]->tokenType)
                  << " right: " << static_cast<uint64_t>(tokenType)
                  << " current pos: " << tokenIterator << std::endl;
        exit(EXIT_FAILURE);
    }
    tokenIterator++;
}

char* Parser::QueryParser::getTableName() {
    Token* token = currentToken();
    assert(isTokenType(TokenType::IDENT));
    char* tableName = (char*)calloc(1, sizeof(char) * 100);
    if (token->ident.has_value()) {
        std::char_traits<char>::copy(tableName, token->ident.value().c_str(),
                                     token->ident.value().size());
    } else {
        debug_error("expected ident at getTableName().\n");
    }
    tokenIterator++;
    return tableName;
}

QueryNode* Parser::QueryParser::stmParse() {
    using namespace Parser;
    QueryNode* query_node = (QueryNode*)calloc(1, sizeof(QueryNode));

    switch (nextToken()->tokenType) {
        case TokenType::SELECT:
            query_node->queryType = QueryType::SELECT;
            query_node->identList = columnParse();
            tokenTypeAssert(TokenType::FROM);
            query_node->tableName = getTableName();
            break;
        case TokenType::CREATE:
            query_node->queryType = QueryType::CREATE;
            tokenTypeAssert(TokenType::TABLE);
            query_node->tableName = getTableName();
            query_node->identList = definitionTableColumnParse();
            break;
        case TokenType::INSERT:
            query_node->queryType = QueryType::INSERT;
            tokenTypeAssert(TokenType::INTO);
            query_node->tableName = getTableName();
            query_node->identList = columnParse();
            tokenTypeAssert(TokenType::VALUES);
            query_node->valueList = valuesParse();
            break;
        case TokenType::DELETE:
            query_node->queryType = QueryType::DELETE;
            tokenTypeAssert(TokenType::FROM);
            query_node->tableName = getTableName();
            tokenTypeAssert(TokenType::WHERE);
            query_node->whereNode = expParse();
            break;
        case TokenType::EXIT:
            query_node->queryType = QueryType::EXIT;
            break;
        default:
            std::cout << "error tokenType: "
                      << static_cast<uint64_t>(tokenList[tokenIterator - 1]->tokenType)
                      << std::endl;

            debug_error("unexpected tokenType.\n");
            break;
    }
    return query_node;
}

ExpNode* Parser::QueryParser::expParse() {
    ExpNode* expNode = (ExpNode*)calloc(1, sizeof(ExpNode));
    expNode->lhs     = unitExpParse();
    switch (nextToken()->tokenType) {
        case TokenType::EQ:
            expNode->expType = ExpType::EQUAL;
            expNode->rhs     = unitExpParse();
            break;
        default:
            debug_error("default error at expParse().\n");
            break;
    }
    return expNode;
}

ExpNode* Parser::QueryParser::unitExpParse() {
    ExpNode* expNode = nullptr;
    switch (nextToken()->tokenType) {
        case TokenType::IDENT:
            expNode          = (ExpNode*)calloc(1, sizeof(ExpNode));
            expNode->expType = ExpType::IDENT;
            assert(currentToken()->ident.has_value());
            std::char_traits<char>::copy(expNode->ident, currentToken()->ident.value().c_str(),
                                         currentToken()->ident.value().size());
            break;
        case TokenType::NUM:
            expNode          = (ExpNode*)calloc(1, sizeof(ExpNode));
            expNode->expType = ExpType::NUM;
            expNode->num     = currentToken()->num;
            break;
        case TokenType::STRING:
            expNode          = (ExpNode*)calloc(1, sizeof(ExpNode));
            expNode->expType = ExpType::STR;
            std::char_traits<char>::copy(expNode->str, currentToken()->str.c_str(),
                                         currentToken()->str.size());
            break;
        default:
            debug_error("default error at unitExpParse().\n");
            break;
    }
    return expNode;
}

IdentList* Parser::QueryParser::columnParse() {
    IdentList* identList = (IdentList*)calloc(1, sizeof(IdentList));

    switch (nextToken()->tokenType) {
        case TokenType::ALLSTAR:
            identList->data_type = NONE;
            identList->ident     = (char*)calloc(1, sizeof("*"));
            std::char_traits<char>::copy(identList->ident, "*", sizeof(char) * 1);
            break;
        case TokenType::LBRACE: {
            IdentList* tail_ident = identList;
            for (;;) {
                Token* token = nextToken();
                assert(token->ident.has_value());
                tail_ident->data_type = NONE;
                tail_ident->ident     = (char*)calloc(1, token->ident.value().size());
                std::char_traits<char>::copy(tail_ident->ident, token->ident.value().c_str(),
                                             token->ident.value().size());
                if (isTokenTypeInc(TokenType::RBRACE)) {
                    break;
                }
                tokenTypeAssert(TokenType::COMMA);
                tail_ident->next = (IdentList*)calloc(1, sizeof(IdentList));
                tail_ident       = tail_ident->next;
            }
        } break;
        default:
            debug_error("default error at columnParse().\n");
            break;
    }
    return identList;
}

std::pair<DataType, uint64_t> Parser::QueryParser::getDataType() {
    std::pair<DataType, uint64_t> dataType;
    switch (nextToken()->tokenType) {
        case TokenType::INT:
            dataType = std::make_pair(DataType::INT, INTEGER_SIZE);
            break;
        case TokenType::CHAR: {
            tokenTypeAssert(TokenType::LBRACE);
            Token* numToken = nextToken();
            assert(numToken->tokenType == TokenType::NUM);
            tokenTypeAssert(TokenType::RBRACE);
            dataType = std::make_pair(DataType::STRING, sizeof(char) * numToken->num);
        } break;
        default:
            debug_error("default error at getDataType().\n");
            break;
    }
    return dataType;
}

IdentList* Parser::QueryParser::definitionTableColumnParse() {
    IdentList* identList = (IdentList*)calloc(1, sizeof(IdentList));

    switch (nextToken()->tokenType) {
        case TokenType::LBRACE: {
            IdentList* tailIdent = identList;
            for (;;) {
                Token* token = nextToken();
                // ident name
                assert(token->ident.has_value());
                tailIdent->ident = (char*)calloc(1, sizeof(char) * token->ident.value().size());
                std::char_traits<char>::copy(tailIdent->ident, token->ident.value().c_str(),
                                             token->ident.value().size());
                // ident type
                auto dataType        = getDataType();
                tailIdent->data_type = dataType.first;
                tailIdent->type_size = (uint16_t)dataType.second;
                // ident attribute
                if (isTokenTypeInc(TokenType::ENCRYPT)) {
                    tailIdent->ident_attribute = IdentAttribute::SECRET;
                } else {
                    tailIdent->ident_attribute = IdentAttribute::NORMAL;
                }

                if (isTokenTypeInc(TokenType::RBRACE)) {
                    break;
                }
                tokenTypeAssert(TokenType::COMMA);
                tailIdent->next = (IdentList*)calloc(1, sizeof(IdentList));
                tailIdent       = tailIdent->next;
            }
        } break;
        default:
            debug_error("default error at columnParse().\n");
            break;
    }
    return identList;
}

ValueList* Parser::QueryParser::valuesParse() {
    ValueList* value_list = NULL;
    switch (nextToken()->tokenType) {
        case TokenType::LBRACE: {
            value_list            = (ValueList*)calloc(1, sizeof(IdentList));
            ValueList* tail_value = value_list;
            for (;;) {
                switch (currentToken()->tokenType) {
                    case TokenType::NUM:
                        tail_value->data_size   = sizeof(int);
                        tail_value->binary_data = (uint8_t*)calloc(1, sizeof(int));
                        memcpy((int*)tail_value->binary_data, &currentToken()->num, sizeof(int));
                        break;
                    case TokenType::STRING: {
                        tail_value->data_size = (uint16_t)currentToken()->str.size();
                        char* data            = (char*)calloc(1, tail_value->data_size);
                        currentToken()->str.copy(data, tail_value->data_size);
                        tail_value->binary_data = (uint8_t*)data;
                        break;
                    }
                    default:
                        debug_error("default error at value at valuesParse().\n");
                        break;
                }
                tokenIteratorInc();
                if (isTokenTypeInc(TokenType::RBRACE)) {
                    break;
                }
                // next value
                tokenTypeAssert(TokenType::COMMA);
                tail_value->next = (ValueList*)calloc(1, sizeof(IdentList));
                tail_value       = tail_value->next;
            }
        } break;
        default:
            debug_error("default error at valuesParse().\n");
            break;
    }
    return value_list;
}

inline unsigned char Parser::QueryParser::currentChar() { return query[charIterator]; }

std::vector<Parser::Token*> Parser::QueryParser::lex() {
    using namespace std;
    using namespace Parser;

    for (; charIterator < query.size();) {
        // ident or reserved word
        if (isalpha(currentChar())) {
            uint64_t startCharId = charIterator;
            Token* token         = new Token();
            for (; isalpha(currentChar()) || isdigit(currentChar()) || currentChar() == '_';)
                charIterator++;
            std::string target = query.substr(startCharId, charIterator - startCharId);
            bool reserved      = false;
            for (auto&& word : RESERVED_WORDS) {
                if (get<0>(word).compare(target) == 0) {
                    token->tokenType = get<1>(word);
                    tokenList.push_back(token);
                    reserved = true;
                    break;
                }
            }
            if (!reserved) {
                token->tokenType = TokenType::IDENT;
                token->ident     = target;
                tokenList.push_back(token);
            }
            continue;
        }
        // number
        if (isdigit(currentChar())) {
            Token* token = new Token();
            uint64_t num = currentChar() - '0';
            ++charIterator;
            for (; isdigit(currentChar()); charIterator++) {
                num = num * 10 + currentChar() - '0';
            }
            token->tokenType = TokenType::NUM;
            token->num       = num;
            tokenList.push_back(token);
            continue;
        }
        // string
        if (currentChar() == '\'' || currentChar() == '\"') {
            Token* token     = new Token();
            uint64_t startId = ++charIterator;
            for (;; ++charIterator) {
                if (currentChar() == '\'' || currentChar() == '\"') {
                    uint64_t strLen  = charIterator - startId;
                    token->tokenType = TokenType::STRING;
                    token->str       = query.substr(startId, strLen);
                    charIterator++;
                    break;
                }
            }
            tokenList.push_back(token);
            continue;
        }
        // signal
        std::string charStr(1, currentChar());
        if (auto iter = SIGNALS.find(charStr); iter != end(SIGNALS)) {
            Token* token     = new Token();
            token->tokenType = iter->second;
            tokenList.push_back(token);
            charIterator++;
            continue;
        }
        // whitespace
        if (isspace(query[charIterator])) {
            charIterator++;
            continue;
        }

        printf("lexer error: %d\n", currentChar());
        exit(EXIT_FAILURE);
    }

    if (PARSE_DEBUG) {
        std::cout << "---LEXER RESULT---" << std::endl;
        std::cout << "[Token list] ";
        for (auto&& token : tokenList) {
            std::cout << magic_enum::enum_name(token->tokenType) << " ";
            if (token->ident.has_value()) std::cout << "ident -> " << token->ident.value() << " ";
            if (token->str.size() > 0) std::cout << "str -> " << token->str << " ";
        }
        std::cout << std::endl;
    }

    return tokenList;
}

void Parser::QueryParser::debugQuery(QueryNode* query_node) {
    std::cout << "QueryType: ";
    switch (query_node->queryType) {
        case QueryType::SELECT:
            std::cout << "select, ";
            break;
        case QueryType::INSERT:
            std::cout << "insert, ";
            break;
        case QueryType::CREATE:
            std::cout << "create, ";
            break;
        case QueryType::DELETE:
            std::cout << "delete, ";
            break;
        default:
            debug_error("query type error at parser debug.\n");
            break;
    }
    std::cout << "TableName: " << query_node->tableName << std::endl;
    debugIdentList(query_node->identList);
    if (query_node->valueList != NULL) {
        debugValueList(query_node->valueList);
    }
    if (query_node->whereNode != NULL) {
        std::cout << "[where] ";
        debugExp(query_node->whereNode);
        std::cout << std::endl;
    }
    std::cout << std::endl;
}

void Parser::QueryParser::debugIdentList(IdentList* ident_list) {
    std::cout << "[Ident List] (";
    for (IdentList* target_ident = ident_list; target_ident != NULL;
         target_ident            = target_ident->next) {
        std::cout << "(";
        std::cout << "ident: " << target_ident->ident << ", ";
        std::cout << "dataType: " << magic_enum::enum_name(target_ident->data_type);
        std::cout << ")";
        if (target_ident->next) std::cout << ", ";
    }
    std::cout << ")";
    std::cout << std::endl;
}

void Parser::QueryParser::debugValueList(ValueList* value_list) {
    std::cout << "[Value List] (";
    for (ValueList* target_value = value_list; target_value != NULL;
         target_value            = target_value->next) {
        std::cout << "(";
        std::cout << "data_size: " << target_value->data_size << ", ";
        std::cout << "data: ";
        for (int i = 0; i < target_value->data_size; i++) std::cout << target_value->binary_data[i];
        std::cout << ")";
        if (target_value->next) std::cout << ", ";
    }
    std::cout << ")";
    std::cout << std::endl;
}

void Parser::QueryParser::debugExp(ExpNode* exp_node) {
    switch (exp_node->expType) {
        case ExpType::EQUAL:
            debugExp(exp_node->lhs);
            std::cout << " EQUAL ";
            debugExp(exp_node->rhs);
            break;
        case ExpType::IDENT:
            std::cout << "IDENT: " << exp_node->ident;
            break;
        case ExpType::NUM:
            std::cout << "NUM: " << exp_node->num;
            break;
        case ExpType::STR:
            std::cout << "STR: " << exp_node->str;
            break;
        default:
            debug_error("expType error at debugExp.\n");
            break;
    }
}