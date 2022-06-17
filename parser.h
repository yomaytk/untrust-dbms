#ifndef _PARSER_H_
#define _PARSER_H_

#include <map>
#include <optional>
#include <string>
#include <tuple>
#include <vector>
#include "Enclave_u.h"

namespace Parser {

enum class TokenType : uint64_t {
    SELECT,
    FROM,
    INSERT,
    INTO,
    VALUES,
    CREATE,
    TABLE,
    DELETE,
    WHERE,
    IDENT,
    STRING,
    NUM,
    SEMI,
    ALLSTAR,
    LBRACE,
    RBRACE,
    COMMA,
    INT,
    CHAR,
    EQ,
    ENCRYPT,
    EXIT,
};

extern std::array<std::tuple<std::string, TokenType>, 13> RESERVED_WORDS;
extern std::map<std::string, TokenType> SIGNALS;

typedef struct {
    TokenType tokenType;
    std::optional<std::string> ident;
    std::string str;
    uint64_t num;
} Token;

// enum class ATTRIBUTE : uint64_t {
//     PLAIN,
//     ENCRYPT,
// };

class QueryParser {
   public:
    std::string query;
    uint64_t charIterator;
    uint64_t tokenIterator;
    std::vector<Token*> tokenList;
    QueryNode* queryNode;
    explicit QueryParser(std::string program);
    virtual ~QueryParser();
    void parse();
    void debugQuery(QueryNode* query_node);

   private:
    std::vector<Token*> lex();
    inline unsigned char currentChar();
    Token* nextToken();
    inline Token* currentToken();
    inline void tokenIteratorInc();
    bool isTokenType(TokenType tokenType);
    bool isTokenTypeInc(TokenType tokenType);
    void tokenTypeAssert(TokenType tokenType);
    char* getTableName();
    std::pair<DataType, uint64_t> getDataType();
    QueryNode* stmParse();
    ExpNode* expParse();
    ExpNode* unitExpParse();
    IdentList* columnParse();
    IdentList* definitionTableColumnParse();
    ValueList* valuesParse();
    void debugIdentList(IdentList* ident_list);
    void debugValueList(ValueList* ident_list);
    void debugExp(ExpNode* exp_node);
};
};  // namespace Parser

#endif