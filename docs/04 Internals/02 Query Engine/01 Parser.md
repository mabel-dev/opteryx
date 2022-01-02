# Query Parser

The parser can be found in the parser folder. This is the starting point for any query
being processed. 

Parsing is broadly split into two steps, one performed by the Tokenizer, the other by
the Lexer.

## Tokenizer

The Tokenizer splits the query string into lexemes - these are units of meaning - which
are generally each word in the query. There are some exceptions to this, such as where
keywords are only valid with another keyword (e.g. `ORDER BY`) or where tokens have
been put in quotes, which generally indicate that anything between those quotes should
we interpretted together as a string literal.

## Lexer

The Lexer assigns meaning to tokens, it does this in two passes. The first is a naive
tagger, which essentially looks for keywords, punctuation and variables.

The second pass builds on this to correct the naive tagger.
