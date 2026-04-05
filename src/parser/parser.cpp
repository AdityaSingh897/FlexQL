#include "flexql/parser.hpp"

#include <cctype>
#include <optional>
#include <string>
#include <vector>

#include "flexql/string_utils.hpp"
#include "flexql/value_utils.hpp"

namespace {

enum class TokenKind {
  Word,
  Number,
  String,
  Symbol,
  Operator,
  End,
};

struct Token {
  TokenKind kind = TokenKind::End;
  std::string text;
};

class Lexer {
 public:
  explicit Lexer(const std::string &input) : input_(input), pos_(0) {}

  std::vector<Token> Scan() {
    std::vector<Token> out;
    while (true) {
      SkipWhitespace();
      if (pos_ >= input_.size()) {
        out.push_back(Token{TokenKind::End, ""});
        break;
      }

      char c = input_[pos_];
      if (std::isalpha(static_cast<unsigned char>(c)) || c == '_') {
        out.push_back(ScanWord());
        continue;
      }
      if (std::isdigit(static_cast<unsigned char>(c)) || (c == '-' && pos_ + 1 < input_.size() && std::isdigit(input_[pos_ + 1]))) {
        out.push_back(ScanNumber());
        continue;
      }
      if (c == '\'' || c == '"') {
        out.push_back(ScanString());
        continue;
      }
      if (c == '<' || c == '>' || c == '!' || c == '=') {
        out.push_back(ScanOperator());
        continue;
      }
      out.push_back(Token{TokenKind::Symbol, std::string(1, c)});
      ++pos_;
    }
    return out;
  }

 private:
  void SkipWhitespace() {
    while (pos_ < input_.size() && std::isspace(static_cast<unsigned char>(input_[pos_]))) {
      ++pos_;
    }
  }

  Token ScanWord() {
    size_t start = pos_;
    ++pos_;
    while (pos_ < input_.size()) {
      const char c = input_[pos_];
      if (!(std::isalnum(static_cast<unsigned char>(c)) || c == '_')) {
        break;
      }
      ++pos_;
    }
    return Token{TokenKind::Word, input_.substr(start, pos_ - start)};
  }

  Token ScanNumber() {
    size_t start = pos_;
    ++pos_;
    while (pos_ < input_.size()) {
      const char c = input_[pos_];
      if (!(std::isdigit(static_cast<unsigned char>(c)) || c == '.')) {
        break;
      }
      ++pos_;
    }
    return Token{TokenKind::Number, input_.substr(start, pos_ - start)};
  }

  Token ScanString() {
    const char quote = input_[pos_];
    size_t start = pos_;
    ++pos_;
    while (pos_ < input_.size()) {
      const char c = input_[pos_];
      if (c == quote) {
        if (pos_ + 1 < input_.size() && input_[pos_ + 1] == quote) {
          pos_ += 2;
          continue;
        }
        ++pos_;
        break;
      }
      ++pos_;
    }
    return Token{TokenKind::String, input_.substr(start, pos_ - start)};
  }

  Token ScanOperator() {
    if (pos_ + 1 < input_.size()) {
      const std::string two = input_.substr(pos_, 2);
      if (two == "<=" || two == ">=" || two == "!=") {
        pos_ += 2;
        return Token{TokenKind::Operator, two};
      }
    }
    const char c = input_[pos_++];
    return Token{TokenKind::Operator, std::string(1, c)};
  }

  const std::string &input_;
  size_t pos_;
};

class Parser {
 public:
  Parser(std::vector<Token> tokens, flexql::QueryError *error) : tokens_(std::move(tokens)), error_(error), idx_(0) {}

  std::optional<flexql::Statement> ParseStatement() {
    if (MatchWord("CREATE")) {
      return ParseCreate();
    }
    if (MatchWord("INSERT")) {
      return ParseInsert();
    }
    if (MatchWord("SELECT")) {
      return ParseSelect();
    }
    return Fail("Unsupported SQL statement");
  }

 private:
  std::optional<flexql::Statement> ParseCreate() {
    if (!ConsumeWord("CREATE") || !ConsumeWord("TABLE")) {
      return Fail("Expected CREATE TABLE");
    }

    const auto table_name = ConsumeIdentifier();
    if (!table_name.has_value()) {
      return Fail("Expected table name");
    }

    if (!ConsumeSymbol("(")) {
      return Fail("Expected '(' after table name");
    }

    std::vector<flexql::ColumnDef> columns;
    bool has_primary = false;

    while (true) {
      const auto col_name = ConsumeIdentifier();
      if (!col_name.has_value()) {
        return Fail("Expected column name");
      }

      const auto type_token = ConsumeIdentifier();
      if (!type_token.has_value()) {
        return Fail("Expected column type");
      }

      const auto type = flexql::ParseColumnType(*type_token);
      if (!type.has_value()) {
        return Fail("Unsupported column type: " + *type_token);
      }

      flexql::ColumnDef col{*col_name, *type, false};

      while (true) {
        if (ConsumeWord("PRIMARY")) {
          if (!ConsumeWord("KEY")) {
            return Fail("Expected KEY after PRIMARY");
          }
          col.primary_key = true;
          has_primary = true;
          continue;
        }
        if (ConsumeWord("NOT")) {
          if (!ConsumeWord("NULL")) {
            return Fail("Expected NULL after NOT");
          }
          continue;
        }
        if (ConsumeWord("NULL")) {
          continue;
        }
        break;
      }

      columns.push_back(std::move(col));

      if (ConsumeSymbol(")")) {
        break;
      }
      if (!ConsumeSymbol(",")) {
        return Fail("Expected ',' between columns");
      }
    }

    if (columns.empty()) {
      return Fail("At least one column is required");
    }
    if (!has_primary) {
      columns[0].primary_key = true;
    }

    if (ConsumeSymbol(";")) {
      ConsumeAllSemicolons();
    }
    if (!AtEnd()) {
      return Fail("Unexpected tokens after CREATE TABLE");
    }

    return flexql::CreateTableStmt{*table_name, std::move(columns)};
  }

  std::optional<flexql::Statement> ParseInsert() {
    if (!ConsumeWord("INSERT") || !ConsumeWord("INTO")) {
      return Fail("Expected INSERT INTO");
    }

    const auto table_name = ConsumeIdentifier();
    if (!table_name.has_value()) {
      return Fail("Expected table name");
    }

    if (!ConsumeWord("VALUES")) {
      return Fail("Expected VALUES (...) in INSERT");
    }

    std::vector<std::vector<std::string>> rows;
    while (true) {
      if (!ConsumeSymbol("(")) {
        return Fail("Expected '(' to start a VALUES tuple");
      }

      std::vector<std::string> values;
      while (true) {
        const auto lit = ConsumeLiteral();
        if (!lit.has_value()) {
          return Fail("Expected literal in VALUES");
        }
        values.push_back(*lit);

        if (ConsumeSymbol(")")) {
          break;
        }
        if (!ConsumeSymbol(",")) {
          return Fail("Expected ',' in VALUES tuple");
        }
      }

      rows.push_back(std::move(values));

      if (!(Peek().kind == TokenKind::Symbol && Peek().text == ",")) {
        break;
      }
      ++idx_;
      if (!(Peek().kind == TokenKind::Symbol && Peek().text == "(")) {
        return Fail("Expected next VALUES tuple after ','");
      }
    }

    std::optional<std::string> expires_literal;
    if (ConsumeWord("EXPIRES")) {
      const auto lit = ConsumeLiteral();
      if (!lit.has_value()) {
        return Fail("Expected expiration timestamp after EXPIRES");
      }
      expires_literal = *lit;
    }

    if (ConsumeSymbol(";")) {
      ConsumeAllSemicolons();
    }
    if (!AtEnd()) {
      return Fail("Unexpected tokens after INSERT");
    }

    return flexql::InsertStmt{*table_name, std::move(rows), expires_literal};
  }

  std::optional<flexql::Statement> ParseSelect() {
    if (!ConsumeWord("SELECT")) {
      return Fail("Expected SELECT");
    }

    std::vector<flexql::SelectItem> items;
    if (ConsumeSymbol("*")) {
      items.push_back(flexql::SelectItem{true, {std::nullopt, "*"}});
    } else {
      while (true) {
        const auto col = ParseQualifiedColumn();
        if (!col.has_value()) {
          return Fail("Expected column in SELECT list");
        }
        items.push_back(flexql::SelectItem{false, *col});
        if (!ConsumeSymbol(",")) {
          break;
        }
      }
    }

    if (!ConsumeWord("FROM")) {
      return Fail("Expected FROM");
    }

    const auto from_table = ConsumeIdentifier();
    if (!from_table.has_value()) {
      return Fail("Expected table name after FROM");
    }

    std::optional<flexql::JoinClause> join;
    if (ConsumeWord("INNER")) {
      if (!ConsumeWord("JOIN")) {
        return Fail("Expected JOIN after INNER");
      }
      const auto right_table = ConsumeIdentifier();
      if (!right_table.has_value()) {
        return Fail("Expected right table name in JOIN");
      }
      if (!ConsumeWord("ON")) {
        return Fail("Expected ON in JOIN clause");
      }

      const auto left_col = ParseQualifiedColumn();
      if (!left_col.has_value()) {
        return Fail("Expected left column in JOIN condition");
      }
      if (!ConsumeOperator("=")) {
        return Fail("JOIN condition must use '='");
      }
      const auto right_col = ParseQualifiedColumn();
      if (!right_col.has_value()) {
        return Fail("Expected right column in JOIN condition");
      }

      join = flexql::JoinClause{*right_table, *left_col, *right_col};
    }

    std::optional<flexql::WhereClause> where;
    if (ConsumeWord("WHERE")) {
      const auto lhs = ParseQualifiedColumn();
      if (!lhs.has_value()) {
        return Fail("Expected column name in WHERE clause");
      }

      const auto op = ConsumeComparison();
      if (!op.has_value()) {
        return Fail("Expected comparison operator in WHERE clause");
      }

      const auto rhs = ConsumeLiteral();
      if (!rhs.has_value()) {
        return Fail("Expected literal value in WHERE clause");
      }

      if (MatchWord("AND") || MatchWord("OR")) {
        return Fail("Only one WHERE condition is supported");
      }

      where = flexql::WhereClause{*lhs, *op, *rhs};
    }

    if (ConsumeSymbol(";")) {
      ConsumeAllSemicolons();
    }
    if (!AtEnd()) {
      return Fail("Unexpected tokens after SELECT");
    }

    return flexql::SelectStmt{std::move(items), *from_table, join, where};
  }

  std::optional<flexql::QualifiedColumn> ParseQualifiedColumn() {
    const auto first = ConsumeIdentifier();
    if (!first.has_value()) {
      return std::nullopt;
    }

    if (ConsumeSymbol(".")) {
      const auto second = ConsumeIdentifier();
      if (!second.has_value()) {
        return std::nullopt;
      }
      return flexql::QualifiedColumn{*first, *second};
    }

    return flexql::QualifiedColumn{std::nullopt, *first};
  }

  std::optional<flexql::CompareOp> ConsumeComparison() {
    if (ConsumeOperator("=")) {
      return flexql::CompareOp::Eq;
    }
    if (ConsumeOperator("!=")) {
      return flexql::CompareOp::Ne;
    }
    if (ConsumeOperator("<=")) {
      return flexql::CompareOp::Le;
    }
    if (ConsumeOperator(">=")) {
      return flexql::CompareOp::Ge;
    }
    if (ConsumeOperator("<")) {
      return flexql::CompareOp::Lt;
    }
    if (ConsumeOperator(">")) {
      return flexql::CompareOp::Gt;
    }
    return std::nullopt;
  }

  std::optional<std::string> ConsumeLiteral() {
    const Token &t = Peek();
    if (t.kind == TokenKind::String || t.kind == TokenKind::Number || t.kind == TokenKind::Word) {
      ++idx_;
      return t.text;
    }
    return std::nullopt;
  }

  std::optional<std::string> ConsumeIdentifier() {
    const Token &t = Peek();
    if (t.kind == TokenKind::Word) {
      ++idx_;
      return t.text;
    }
    return std::nullopt;
  }

  bool ConsumeWord(const std::string &word) {
    const Token &t = Peek();
    if (t.kind != TokenKind::Word) {
      return false;
    }
    if (!flexql::IEquals(t.text, word)) {
      return false;
    }
    ++idx_;
    return true;
  }

  bool ConsumeSymbol(const std::string &symbol) {
    const Token &t = Peek();
    if (t.kind == TokenKind::Symbol && t.text == symbol) {
      ++idx_;
      return true;
    }
    return false;
  }

  bool ConsumeOperator(const std::string &op) {
    const Token &t = Peek();
    if (t.kind == TokenKind::Operator && t.text == op) {
      ++idx_;
      return true;
    }
    return false;
  }

  bool MatchWord(const std::string &word) const {
    const Token &t = Peek();
    if (t.kind != TokenKind::Word) {
      return false;
    }
    return flexql::IEquals(t.text, word);
  }

  void ConsumeAllSemicolons() {
    while (ConsumeSymbol(";")) {
    }
  }

  bool AtEnd() const {
    return Peek().kind == TokenKind::End;
  }

  std::optional<flexql::Statement> Fail(const std::string &msg) {
    if (error_ != nullptr) {
      error_->message = msg;
    }
    return std::nullopt;
  }

  const Token &Peek() const {
    return tokens_[idx_ < tokens_.size() ? idx_ : (tokens_.size() - 1)];
  }

  std::vector<Token> tokens_;
  flexql::QueryError *error_;
  size_t idx_;
};

}  // namespace

namespace flexql {

std::optional<Statement> SqlParser::Parse(const std::string &sql, QueryError *error) {
  Lexer lexer(sql);
  Parser parser(lexer.Scan(), error);
  return parser.ParseStatement();
}

}  // namespace flexql
