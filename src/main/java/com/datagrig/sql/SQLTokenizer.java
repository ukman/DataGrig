package com.datagrig.sql;

import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ukman on 10/30/18.
 */
@Slf4j
public class SQLTokenizer {

    @AllArgsConstructor
    @Builder
    @Data
    public static class Token {

        private String token;

        private SQLTokenType type;

        public String toString() {
            return token;
        }
    }

    @AllArgsConstructor
    @NoArgsConstructor
    public enum SQLTokenType {
        KEYWORD("select|from|where|join|left|on|order|by|group|like|is|null|distinct|as"),
        ID("[$A-Za-z_][$A-Za-z_0-9]*|\\?"),
        ASTERISK("\\*"),
        SPACE("\\s+"),
        STRING("\"[^\"]*\"|'[^']*'"),
        NUMBER("\\.[0-9]+|[0-9]+\\.[0-9]*|[0-9]+"),
        CATALOGSEPARATOR("\\."),
//        POINT("->"),
//        POINT2("->>"),
//        POINT_ARRAY("#>"),
//        POINT_ARRAY2("#>>"),
        COMMENT("#[.]*"),
        LESS("\\<"),
        LE("\\<="),
        GREATER("\\>"),
        GE("\\>="),
        EQ("="),
        MINUS("\\-"),
        PLUS("\\+"),
        MULT("\\*"),
        DIV("/"),
        MOD("\\\\"),
        COMMA("\\,"),
        OPENBRACE("\\("),
        CLOSEBRACE("\\)"),
        QUESTION("\\?"),
        COLON("\\:"),
        DOUBLECOLON("\\:\\:"),
        UNKNOWN("."),
        ;
        private String regex;

        public static SQLTokenType typeOf(Matcher m) {
            String full = m.group();
            for(SQLTokenType type : values()) {
                String s = m.group(type.name());
                if(s != null && s.equals(full)) {
                    return type;
                }
            }
            return null;
        }

    }

    public SQLTokenizer() throws SQLException {
        this(null);
    }

    public SQLTokenizer(DatabaseMetaData metadata) throws SQLException {
        StringBuilder regex = new StringBuilder();
        for(SQLTokenType type : SQLTokenType.values()) {
            if(regex.length() > 0) {
                regex.append("|");
            }
            regex.append("(?");
            regex.append("<" + type.name() + ">");
            String typeRegex = type.regex;
            if(metadata != null) {
                if (type == SQLTokenType.KEYWORD) {
                    String keyWords = metadata.getSQLKeywords();
                    typeRegex = keyWords.replace(',', '|');
                } else if (type == SQLTokenType.ID) {
                    String singleId = "(?:" + type.regex + "|" + metadata.getIdentifierQuoteString() + type.regex + metadata.getIdentifierQuoteString() + ")";
                    typeRegex = singleId +
                            "(?:\\" + metadata.getCatalogSeparator() + singleId + ")*";
                } else if (type == SQLTokenType.CATALOGSEPARATOR) {
                    typeRegex = metadata.getCatalogSeparator();
                }
            } else {
                if (type == SQLTokenType.ID) {
                    String singleId = "(?:" + type.regex + ")";
                    typeRegex = singleId +
                            "(?:\\." + singleId + ")*";
                }
            }
            regex.append(typeRegex);
            regex.append(")");
        }
        pattern = Pattern.compile(regex.toString(), Pattern.CASE_INSENSITIVE);
    }

    private Pattern pattern;
    public List<Token> getTokens(String sql) {
        List<Token> res = new ArrayList<>();
        Matcher m = pattern.matcher(sql);
        int start = 0;
        while(m.find(start) && start < sql.length()) {
            String token = m.group();
            SQLTokenType type = SQLTokenType.typeOf(m);
            // log.debug("'" + token + "' - " + SQLTokenType.typeOf(m));
            if(m.start() > start) {
                throw new IllegalStateException("Cannot parse starting from '" + sql.substring(start) + "'");
            }
            res.add(Token.builder()
                    .token(token)
                    .type(type)
                    .build());
            start += token.length();
        }
        return res;
    }

    /*
    public static void main(String[] args) throws SQLException {
        SQLTokenizer tokenizer = new SQLTokenizer();
        List<Token> tokens = tokenizer.getTokens("SELECT * from table where a like 'Hello%eee' and t.b=1 and c=1. and d=1.4 and f=.45 or g=? or h > 5 or g < 3 or (gggg>=6 or z <= 45 or bb=:bb)");
        log.debug("Tokens = " + tokens);
    }
    //*/
}
