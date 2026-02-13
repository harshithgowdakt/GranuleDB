package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// Parser is a recursive descent SQL parser.
type Parser struct {
	tokens []Token
	pos    int
}

// NewParser creates a parser from a slice of tokens.
func NewParser(tokens []Token) *Parser {
	return &Parser{tokens: tokens, pos: 0}
}

// Parse parses the token stream into a statement.
func (p *Parser) Parse() (Statement, error) {
	tok := p.peek()
	switch tok.Type {
	case TokenCREATE:
		return p.parseCreate()
	case TokenINSERT:
		return p.parseInsert()
	case TokenSELECT:
		return p.parseSelect()
	case TokenDROP:
		return p.parseDrop()
	case TokenSHOW:
		return p.parseShow()
	default:
		return nil, p.errorf("unexpected token %q, expected a statement", tok.Literal)
	}
}

func (p *Parser) parseCreate() (Statement, error) {
	// CREATE MATERIALIZED VIEW ...
	if p.peekAt(p.pos+1).Type == TokenMATERIALIZED {
		return p.parseCreateMaterializedView()
	}
	// CREATE TABLE ...
	return p.parseCreateTable()
}

// ParseSQL is a convenience function: lex + parse a SQL string.
func ParseSQL(sql string) (Statement, error) {
	lexer := NewLexer(sql)
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil, err
	}
	parser := NewParser(tokens)
	return parser.Parse()
}

// --- Token helpers ---

func (p *Parser) peek() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[p.pos]
}

func (p *Parser) advance() Token {
	tok := p.peek()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return tok
}

func (p *Parser) expect(tt TokenType) (Token, error) {
	tok := p.advance()
	if tok.Type != tt {
		return tok, p.errorf("expected token type %d, got %q (%d)", tt, tok.Literal, tok.Type)
	}
	return tok, nil
}

func (p *Parser) expectKeyword(tt TokenType) error {
	_, err := p.expect(tt)
	return err
}

func (p *Parser) match(tt TokenType) bool {
	if p.peek().Type == tt {
		p.advance()
		return true
	}
	return false
}

func (p *Parser) errorf(format string, args ...interface{}) error {
	tok := p.peek()
	prefix := fmt.Sprintf("line %d col %d: ", tok.Line, tok.Col)
	return fmt.Errorf(prefix+format, args...)
}

// --- CREATE TABLE ---

func (p *Parser) parseCreateTable() (*CreateTableStmt, error) {
	if err := p.expectKeyword(TokenCREATE); err != nil {
		return nil, err
	}
	if err := p.expectKeyword(TokenTABLE); err != nil {
		return nil, err
	}

	stmt := &CreateTableStmt{}

	// IF NOT EXISTS
	if p.peek().Type == TokenIF {
		p.advance()
		if err := p.expectKeyword(TokenNOT); err != nil {
			return nil, err
		}
		if err := p.expectKeyword(TokenEXISTS); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}

	// Table name
	nameTok, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.TableName = nameTok.Literal

	// Column definitions: ( col1 Type1, col2 Type2, ... )
	if _, err := p.expect(TokenLParen); err != nil {
		return nil, err
	}

	for {
		colName, err := p.expect(TokenIdentifier)
		if err != nil {
			return nil, err
		}
		colType, err := p.expect(TokenIdentifier)
		if err != nil {
			return nil, err
		}
		typeName := colType.Literal
		// Handle LowCardinality(InnerType)
		if strings.ToLower(typeName) == "lowcardinality" {
			if _, err := p.expect(TokenLParen); err != nil {
				return nil, err
			}
			innerType, err := p.expect(TokenIdentifier)
			if err != nil {
				return nil, err
			}
			if _, err := p.expect(TokenRParen); err != nil {
				return nil, err
			}
			typeName = "LowCardinality(" + innerType.Literal + ")"
		} else if strings.ToLower(typeName) == "aggregatefunction" {
			// Consume AggregateFunction(...) and keep normalized type marker.
			if _, err := p.expect(TokenLParen); err != nil {
				return nil, err
			}
			depth := 1
			for depth > 0 {
				tok := p.advance()
				if tok.Type == TokenEOF {
					return nil, p.errorf("unterminated AggregateFunction type")
				}
				if tok.Type == TokenLParen {
					depth++
				} else if tok.Type == TokenRParen {
					depth--
				}
			}
			typeName = "AggregateFunction"
		}
		stmt.Columns = append(stmt.Columns, ColumnDefNode{
			Name:     colName.Literal,
			TypeName: typeName,
		})
		if !p.match(TokenComma) {
			break
		}
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	// ENGINE = MergeTree()
	if err := p.expectKeyword(TokenENGINE); err != nil {
		return nil, err
	}
	if _, err := p.expect(TokenEQ); err != nil {
		return nil, err
	}
	engineTok := p.advance()
	stmt.Engine = engineTok.Literal

	// Optional ()
	if p.match(TokenLParen) {
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
	}

	// ORDER BY (col1, col2, ...) or ORDER BY col1
	if p.peek().Type == TokenORDER {
		p.advance()
		if err := p.expectKeyword(TokenBY); err != nil {
			return nil, err
		}
		stmt.OrderBy, err = p.parseColumnList()
		if err != nil {
			return nil, err
		}
	}

	// PARTITION BY expr
	if p.peek().Type == TokenPARTITION {
		p.advance()
		if err := p.expectKeyword(TokenBY); err != nil {
			return nil, err
		}
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.PartitionBy = expr
	}

	return stmt, nil
}

func (p *Parser) parseCreateMaterializedView() (*CreateMaterializedViewStmt, error) {
	if err := p.expectKeyword(TokenCREATE); err != nil {
		return nil, err
	}
	if err := p.expectKeyword(TokenMATERIALIZED); err != nil {
		return nil, err
	}
	if err := p.expectKeyword(TokenVIEW); err != nil {
		return nil, err
	}

	stmt := &CreateMaterializedViewStmt{}

	// Optional IF NOT EXISTS.
	if p.peek().Type == TokenIF {
		p.advance()
		if err := p.expectKeyword(TokenNOT); err != nil {
			return nil, err
		}
		if err := p.expectKeyword(TokenEXISTS); err != nil {
			return nil, err
		}
		stmt.IfNotExists = true
	}

	nameTok, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.ViewName = nameTok.Literal

	if err := p.expectKeyword(TokenTO); err != nil {
		return nil, err
	}
	targetTok, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.TargetTable = targetTok.Literal

	if err := p.expectKeyword(TokenAS); err != nil {
		return nil, err
	}

	selectStmt, err := p.parseSelect()
	if err != nil {
		return nil, err
	}
	stmt.Select = selectStmt

	return stmt, nil
}

// --- INSERT ---

func (p *Parser) parseInsert() (*InsertStmt, error) {
	if err := p.expectKeyword(TokenINSERT); err != nil {
		return nil, err
	}
	if err := p.expectKeyword(TokenINTO); err != nil {
		return nil, err
	}

	nameTok, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt := &InsertStmt{TableName: nameTok.Literal}

	// Optional column list
	if p.peek().Type == TokenLParen {
		p.advance()
		for {
			colTok, err := p.expect(TokenIdentifier)
			if err != nil {
				return nil, err
			}
			stmt.Columns = append(stmt.Columns, colTok.Literal)
			if !p.match(TokenComma) {
				break
			}
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
	}

	// VALUES
	if err := p.expectKeyword(TokenVALUES); err != nil {
		return nil, err
	}

	// Parse row value lists: (v1, v2), (v3, v4), ...
	for {
		if _, err := p.expect(TokenLParen); err != nil {
			return nil, err
		}
		var row []Expression
		for {
			expr, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			row = append(row, expr)
			if !p.match(TokenComma) {
				break
			}
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
		stmt.Values = append(stmt.Values, row)
		if !p.match(TokenComma) {
			break
		}
	}

	return stmt, nil
}

// --- SELECT ---

func (p *Parser) parseSelect() (*SelectStmt, error) {
	if err := p.expectKeyword(TokenSELECT); err != nil {
		return nil, err
	}

	stmt := &SelectStmt{}

	// SELECT list
	for {
		se, err := p.parseSelectExpr()
		if err != nil {
			return nil, err
		}
		stmt.Columns = append(stmt.Columns, se)
		if !p.match(TokenComma) {
			break
		}
	}

	// FROM
	if p.peek().Type == TokenFROM {
		p.advance()
		nameTok, err := p.expect(TokenIdentifier)
		if err != nil {
			return nil, err
		}
		stmt.From = nameTok.Literal
	}

	// WHERE
	if p.peek().Type == TokenWHERE {
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		stmt.Where = expr
	}

	// GROUP BY
	if p.peek().Type == TokenGROUP {
		p.advance()
		if err := p.expectKeyword(TokenBY); err != nil {
			return nil, err
		}
		for {
			colTok, err := p.expect(TokenIdentifier)
			if err != nil {
				return nil, err
			}
			stmt.GroupBy = append(stmt.GroupBy, colTok.Literal)
			if !p.match(TokenComma) {
				break
			}
		}
	}

	// HAVING (parsed but stored in Where for simplicity as an AND condition)
	// For now, skip HAVING support

	// ORDER BY
	if p.peek().Type == TokenORDER {
		p.advance()
		if err := p.expectKeyword(TokenBY); err != nil {
			return nil, err
		}
		for {
			colTok, err := p.expect(TokenIdentifier)
			if err != nil {
				return nil, err
			}
			desc := false
			if p.peek().Type == TokenDESC {
				p.advance()
				desc = true
			} else if p.peek().Type == TokenASC {
				p.advance()
			}
			stmt.OrderBy = append(stmt.OrderBy, OrderByExpr{Column: colTok.Literal, Desc: desc})
			if !p.match(TokenComma) {
				break
			}
		}
	}

	// LIMIT
	if p.peek().Type == TokenLIMIT {
		p.advance()
		numTok, err := p.expect(TokenNumber)
		if err != nil {
			return nil, err
		}
		n, err := strconv.ParseInt(numTok.Literal, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid LIMIT value: %s", numTok.Literal)
		}
		stmt.Limit = &n
	}

	return stmt, nil
}

func (p *Parser) parseSelectExpr() (SelectExpr, error) {
	// Check for *
	if p.peek().Type == TokenStar {
		p.advance()
		return SelectExpr{Expr: &StarExpr{}}, nil
	}

	expr, err := p.parseExpression()
	if err != nil {
		return SelectExpr{}, err
	}

	var alias string
	if p.peek().Type == TokenAS {
		p.advance()
		aliasTok, err := p.expect(TokenIdentifier)
		if err != nil {
			return SelectExpr{}, err
		}
		alias = aliasTok.Literal
	} else if p.peek().Type == TokenIdentifier {
		// Allow alias without AS for simple cases
		// But only if the next token is a comma, FROM, or EOF (not an operator)
		nextTok := p.peek()
		nextNext := p.peekAt(p.pos + 1)
		if nextNext.Type == TokenComma || nextNext.Type == TokenFROM ||
			nextNext.Type == TokenEOF || nextNext.Type == TokenSemicolon ||
			nextNext.Type == TokenWHERE || nextNext.Type == TokenGROUP ||
			nextNext.Type == TokenORDER || nextNext.Type == TokenLIMIT {
			alias = nextTok.Literal
			p.advance()
		}
	}

	return SelectExpr{Expr: expr, Alias: alias}, nil
}

func (p *Parser) peekAt(pos int) Token {
	if pos >= len(p.tokens) {
		return Token{Type: TokenEOF}
	}
	return p.tokens[pos]
}

// --- DROP TABLE ---

func (p *Parser) parseDrop() (*DropTableStmt, error) {
	if err := p.expectKeyword(TokenDROP); err != nil {
		return nil, err
	}
	if err := p.expectKeyword(TokenTABLE); err != nil {
		return nil, err
	}

	stmt := &DropTableStmt{}

	if p.peek().Type == TokenIF {
		p.advance()
		if err := p.expectKeyword(TokenEXISTS); err != nil {
			return nil, err
		}
		stmt.IfExists = true
	}

	nameTok, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	stmt.TableName = nameTok.Literal
	return stmt, nil
}

// --- SHOW TABLES ---

func (p *Parser) parseShow() (*ShowTablesStmt, error) {
	if err := p.expectKeyword(TokenSHOW); err != nil {
		return nil, err
	}
	if err := p.expectKeyword(TokenTABLES); err != nil {
		return nil, err
	}
	return &ShowTablesStmt{}, nil
}

// --- Expression parsing (recursive descent with precedence) ---
// Precedence (lowest to highest): OR, AND, NOT, comparison, addition, multiplication, unary, primary

func (p *Parser) parseExpression() (Expression, error) {
	return p.parseOr()
}

func (p *Parser) parseOr() (Expression, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.peek().Type == TokenOR {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: "OR", Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseAnd() (Expression, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.peek().Type == TokenAND {
		p.advance()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: "AND", Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseNot() (Expression, error) {
	if p.peek().Type == TokenNOT {
		p.advance()
		expr, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: "NOT", Expr: expr}, nil
	}
	return p.parseComparison()
}

func (p *Parser) parseComparison() (Expression, error) {
	left, err := p.parseAddSub()
	if err != nil {
		return nil, err
	}

	switch p.peek().Type {
	case TokenEQ, TokenNEQ, TokenLT, TokenGT, TokenLTE, TokenGTE:
		op := p.advance().Literal
		right, err := p.parseAddSub()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Op: op, Left: left, Right: right}, nil
	}
	return left, nil
}

func (p *Parser) parseAddSub() (Expression, error) {
	left, err := p.parseMulDiv()
	if err != nil {
		return nil, err
	}
	for p.peek().Type == TokenPlus || p.peek().Type == TokenMinus {
		op := p.advance().Literal
		right, err := p.parseMulDiv()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseMulDiv() (Expression, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.peek().Type == TokenStar || p.peek().Type == TokenSlash {
		op := p.advance().Literal
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &BinaryExpr{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *Parser) parseUnary() (Expression, error) {
	if p.peek().Type == TokenMinus {
		p.advance()
		expr, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{Op: "-", Expr: expr}, nil
	}
	return p.parsePrimary()
}

func (p *Parser) parsePrimary() (Expression, error) {
	tok := p.peek()

	switch tok.Type {
	case TokenNumber:
		p.advance()
		if strings.Contains(tok.Literal, ".") {
			f, err := strconv.ParseFloat(tok.Literal, 64)
			if err != nil {
				return nil, err
			}
			return &LiteralExpr{Value: f}, nil
		}
		n, err := strconv.ParseInt(tok.Literal, 10, 64)
		if err != nil {
			return nil, err
		}
		return &LiteralExpr{Value: n}, nil

	case TokenString:
		p.advance()
		return &LiteralExpr{Value: tok.Literal}, nil

	case TokenStar:
		p.advance()
		return &StarExpr{}, nil

	case TokenIdentifier:
		p.advance()
		// Check if this is a function call
		if p.peek().Type == TokenLParen {
			return p.parseFunctionCall(tok.Literal)
		}
		return &ColumnRef{Name: tok.Literal}, nil

	case TokenLParen:
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
		return expr, nil

	default:
		return nil, p.errorf("unexpected token %q in expression", tok.Literal)
	}
}

func (p *Parser) parseFunctionCall(name string) (Expression, error) {
	p.advance() // consume (

	var args []Expression
	if p.peek().Type != TokenRParen {
		for {
			arg, err := p.parseExpression()
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
			if !p.match(TokenComma) {
				break
			}
		}
	}

	if _, err := p.expect(TokenRParen); err != nil {
		return nil, err
	}

	return &FunctionCall{Name: strings.ToLower(name), Args: args}, nil
}

// parseColumnList parses either (col1, col2) or a single col.
func (p *Parser) parseColumnList() ([]string, error) {
	if p.match(TokenLParen) {
		var cols []string
		for {
			tok, err := p.expect(TokenIdentifier)
			if err != nil {
				return nil, err
			}
			cols = append(cols, tok.Literal)
			if !p.match(TokenComma) {
				break
			}
		}
		if _, err := p.expect(TokenRParen); err != nil {
			return nil, err
		}
		return cols, nil
	}

	// Single column
	tok, err := p.expect(TokenIdentifier)
	if err != nil {
		return nil, err
	}
	return []string{tok.Literal}, nil
}

// ParseExpression parses a standalone SQL expression string into an AST Expression.
func ParseExpression(sql string) (Expression, error) {
	lexer := NewLexer(sql)
	tokens, err := lexer.Tokenize()
	if err != nil {
		return nil, err
	}
	p := NewParser(tokens)
	expr, err := p.parseExpression()
	if err != nil {
		return nil, err
	}
	if p.peek().Type != TokenEOF && p.peek().Type != TokenSemicolon {
		return nil, fmt.Errorf("unexpected token after expression: %q", p.peek().Literal)
	}
	return expr, nil
}
