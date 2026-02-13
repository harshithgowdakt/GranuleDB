package parser

import "fmt"

// Lexer tokenizes SQL input.
type Lexer struct {
	input []byte
	pos   int
	line  int
	col   int
}

// NewLexer creates a new lexer for the given input.
func NewLexer(input string) *Lexer {
	return &Lexer{
		input: []byte(input),
		pos:   0,
		line:  1,
		col:   1,
	}
}

// Tokenize returns all tokens from the input.
func (l *Lexer) Tokenize() ([]Token, error) {
	var tokens []Token
	for {
		tok, err := l.NextToken()
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, tok)
		if tok.Type == TokenEOF {
			break
		}
	}
	return tokens, nil
}

// NextToken returns the next token from the input.
func (l *Lexer) NextToken() (Token, error) {
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return Token{Type: TokenEOF, Line: l.line, Col: l.col}, nil
	}

	ch := l.input[l.pos]
	line, col := l.line, l.col

	switch {
	case ch == '(':
		l.advance()
		return Token{Type: TokenLParen, Literal: "(", Line: line, Col: col}, nil
	case ch == ')':
		l.advance()
		return Token{Type: TokenRParen, Literal: ")", Line: line, Col: col}, nil
	case ch == ',':
		l.advance()
		return Token{Type: TokenComma, Literal: ",", Line: line, Col: col}, nil
	case ch == '*':
		l.advance()
		return Token{Type: TokenStar, Literal: "*", Line: line, Col: col}, nil
	case ch == '+':
		l.advance()
		return Token{Type: TokenPlus, Literal: "+", Line: line, Col: col}, nil
	case ch == '-':
		// Check if this is a negative number
		if l.pos+1 < len(l.input) && isDigit(l.input[l.pos+1]) {
			return l.readNumber()
		}
		l.advance()
		return Token{Type: TokenMinus, Literal: "-", Line: line, Col: col}, nil
	case ch == '/':
		l.advance()
		return Token{Type: TokenSlash, Literal: "/", Line: line, Col: col}, nil
	case ch == '.':
		l.advance()
		return Token{Type: TokenDot, Literal: ".", Line: line, Col: col}, nil
	case ch == ';':
		l.advance()
		return Token{Type: TokenSemicolon, Literal: ";", Line: line, Col: col}, nil
	case ch == '=':
		l.advance()
		return Token{Type: TokenEQ, Literal: "=", Line: line, Col: col}, nil
	case ch == '!':
		l.advance()
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.advance()
			return Token{Type: TokenNEQ, Literal: "!=", Line: line, Col: col}, nil
		}
		return Token{}, fmt.Errorf("line %d col %d: unexpected character '!'", line, col)
	case ch == '<':
		l.advance()
		if l.pos < len(l.input) {
			if l.input[l.pos] == '=' {
				l.advance()
				return Token{Type: TokenLTE, Literal: "<=", Line: line, Col: col}, nil
			}
			if l.input[l.pos] == '>' {
				l.advance()
				return Token{Type: TokenNEQ, Literal: "<>", Line: line, Col: col}, nil
			}
		}
		return Token{Type: TokenLT, Literal: "<", Line: line, Col: col}, nil
	case ch == '>':
		l.advance()
		if l.pos < len(l.input) && l.input[l.pos] == '=' {
			l.advance()
			return Token{Type: TokenGTE, Literal: ">=", Line: line, Col: col}, nil
		}
		return Token{Type: TokenGT, Literal: ">", Line: line, Col: col}, nil
	case ch == '\'':
		return l.readString()
	case isDigit(ch):
		return l.readNumber()
	case isIdentStart(ch):
		return l.readIdentifier()
	default:
		return Token{}, fmt.Errorf("line %d col %d: unexpected character '%c'", line, col, ch)
	}
}

func (l *Lexer) advance() {
	if l.pos < len(l.input) {
		if l.input[l.pos] == '\n' {
			l.line++
			l.col = 1
		} else {
			l.col++
		}
		l.pos++
	}
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n' {
			l.advance()
		} else if ch == '-' && l.pos+1 < len(l.input) && l.input[l.pos+1] == '-' {
			// Line comment
			for l.pos < len(l.input) && l.input[l.pos] != '\n' {
				l.advance()
			}
		} else {
			break
		}
	}
}

func (l *Lexer) readString() (Token, error) {
	line, col := l.line, l.col
	l.advance() // skip opening quote

	var literal []byte
	for l.pos < len(l.input) {
		ch := l.input[l.pos]
		if ch == '\'' {
			// Check for escaped quote ''
			if l.pos+1 < len(l.input) && l.input[l.pos+1] == '\'' {
				literal = append(literal, '\'')
				l.advance()
				l.advance()
				continue
			}
			l.advance() // skip closing quote
			return Token{Type: TokenString, Literal: string(literal), Line: line, Col: col}, nil
		}
		if ch == '\\' && l.pos+1 < len(l.input) {
			l.advance()
			switch l.input[l.pos] {
			case '\\':
				literal = append(literal, '\\')
			case '\'':
				literal = append(literal, '\'')
			case 'n':
				literal = append(literal, '\n')
			case 't':
				literal = append(literal, '\t')
			default:
				literal = append(literal, l.input[l.pos])
			}
			l.advance()
			continue
		}
		literal = append(literal, ch)
		l.advance()
	}
	return Token{}, fmt.Errorf("line %d col %d: unterminated string", line, col)
}

func (l *Lexer) readNumber() (Token, error) {
	line, col := l.line, l.col
	start := l.pos

	if l.input[l.pos] == '-' {
		l.advance()
	}

	for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
		l.advance()
	}

	// Check for decimal point
	if l.pos < len(l.input) && l.input[l.pos] == '.' {
		l.advance()
		for l.pos < len(l.input) && isDigit(l.input[l.pos]) {
			l.advance()
		}
	}

	return Token{Type: TokenNumber, Literal: string(l.input[start:l.pos]), Line: line, Col: col}, nil
}

func (l *Lexer) readIdentifier() (Token, error) {
	line, col := l.line, l.col
	start := l.pos

	for l.pos < len(l.input) && isIdentPart(l.input[l.pos]) {
		l.advance()
	}

	literal := string(l.input[start:l.pos])
	tt := LookupKeyword(literal)

	return Token{Type: tt, Literal: literal, Line: line, Col: col}, nil
}

func isDigit(ch byte) bool { return ch >= '0' && ch <= '9' }
func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}
func isIdentPart(ch byte) bool { return isIdentStart(ch) || isDigit(ch) }
