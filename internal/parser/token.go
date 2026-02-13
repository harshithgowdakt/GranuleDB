package parser

// TokenType represents the type of a lexical token.
type TokenType int

const (
	// Literals
	TokenIdentifier TokenType = iota
	TokenNumber               // integer or float literal
	TokenString               // 'single-quoted string'

	// Keywords
	TokenSELECT
	TokenFROM
	TokenWHERE
	TokenORDER
	TokenBY
	TokenLIMIT
	TokenGROUP
	TokenHAVING
	TokenINSERT
	TokenINTO
	TokenVALUES
	TokenCREATE
	TokenTABLE
	TokenVIEW
	TokenENGINE
	TokenMergeTree
	TokenMATERIALIZED
	TokenPARTITION
	TokenTO
	TokenAS
	TokenAND
	TokenOR
	TokenNOT
	TokenIN
	TokenBETWEEN
	TokenASC
	TokenDESC
	TokenDROP
	TokenSHOW
	TokenTABLES
	TokenIF
	TokenEXISTS

	// Operators and punctuation
	TokenLParen    // (
	TokenRParen    // )
	TokenComma     // ,
	TokenStar      // *
	TokenEQ        // =
	TokenNEQ       // != or <>
	TokenLT        // <
	TokenGT        // >
	TokenLTE       // <=
	TokenGTE       // >=
	TokenPlus      // +
	TokenMinus     // -
	TokenSlash     // /
	TokenDot       // .
	TokenSemicolon // ;

	TokenEOF
)

// Token represents a lexical token.
type Token struct {
	Type    TokenType
	Literal string
	Line    int
	Col     int
}

var keywords = map[string]TokenType{
	"SELECT":       TokenSELECT,
	"FROM":         TokenFROM,
	"WHERE":        TokenWHERE,
	"ORDER":        TokenORDER,
	"BY":           TokenBY,
	"LIMIT":        TokenLIMIT,
	"GROUP":        TokenGROUP,
	"HAVING":       TokenHAVING,
	"INSERT":       TokenINSERT,
	"INTO":         TokenINTO,
	"VALUES":       TokenVALUES,
	"CREATE":       TokenCREATE,
	"TABLE":        TokenTABLE,
	"VIEW":         TokenVIEW,
	"ENGINE":       TokenENGINE,
	"MERGETREE":    TokenMergeTree,
	"MATERIALIZED": TokenMATERIALIZED,
	"PARTITION":    TokenPARTITION,
	"TO":           TokenTO,
	"AS":           TokenAS,
	"AND":          TokenAND,
	"OR":           TokenOR,
	"NOT":          TokenNOT,
	"IN":           TokenIN,
	"BETWEEN":      TokenBETWEEN,
	"ASC":          TokenASC,
	"DESC":         TokenDESC,
	"DROP":         TokenDROP,
	"SHOW":         TokenSHOW,
	"TABLES":       TokenTABLES,
	"IF":           TokenIF,
	"EXISTS":       TokenEXISTS,
}

// LookupKeyword returns the keyword token type for an identifier, or TokenIdentifier.
func LookupKeyword(ident string) TokenType {
	// Case-insensitive lookup
	upper := toUpper(ident)
	if tt, ok := keywords[upper]; ok {
		return tt
	}
	return TokenIdentifier
}

func toUpper(s string) string {
	b := make([]byte, len(s))
	for i := range len(s) {
		c := s[i]
		if c >= 'a' && c <= 'z' {
			b[i] = c - 32
		} else {
			b[i] = c
		}
	}
	return string(b)
}
