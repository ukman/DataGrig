package com.datagrig.sql;

import com.akiban.sql.parser.FromList;
import org.apache.commons.collections.ListUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.datagrig.sql.SQLTokenizer.SQLTokenType.*;
import static com.datagrig.sql.SQLTokenizer.*;

/**
 * Created by ukman on 11/1/18.
 */
public class SQLParser extends SQLAbstractParser{
    private static final Token WHERE = new Token("where", null);
    private static final Token GROUP = new Token("group", null);
    private static final Token ORDER = new Token("order", null);
    private static final Token HAVING = new Token("having", null);
    private static final Token LIMIT = new Token("limit", null);

    private static final List<SQLTokenizer.Token> AFTER_FROM_TOKENS = Arrays.asList(WHERE, GROUP, ORDER, HAVING, LIMIT);

    private static final List<SQLTokenizer.Token> JOIN_TOKENS = Arrays.asList(
            new SQLTokenizer.Token("join", null),
            new SQLTokenizer.Token("left", null),
            new SQLTokenizer.Token("right", null),
            new SQLTokenizer.Token("outer", null),
            new SQLTokenizer.Token("inner", null),
            new SQLTokenizer.Token(null, COMMA)
    );

    private static final List<SQLTokenizer.Token> JOIN_TOKENS_WITHSPACE = ListUtils.union(JOIN_TOKENS,
            Arrays.asList(new SQLTokenizer.Token(null, SPACE))
    );

    private static final List<SQLTokenizer.Token> JOIN_TOKENS_WITH_WHERE_GROUP_ORDER = ListUtils.union(JOIN_TOKENS, AFTER_FROM_TOKENS);



    public SQLQuery parse(String sql) throws SQLParseException, SQLException {
        SQLTokenizer tokenizer = new SQLTokenizer();
        List<SQLTokenizer.Token> tokens = tokenizer.getTokens(sql);
        return parse(tokens);
    }

    public SQLQuery parse(List<SQLTokenizer.Token> tokens) throws SQLParseException {
        int idx = 0;
        List<SelectField> selectFieldList = new ArrayList<>();
        List<FromTable> fromList = new ArrayList<>();
        Where where = null;
        while(idx < tokens.size()) {
            idx = skipSpaces(tokens, idx);
            SQLTokenizer.Token t = tokens.get(idx);
            if(t.getType() == KEYWORD && t.getToken().equalsIgnoreCase("select")) {
                idx = parseSelectList(tokens, idx, selectFieldList);
            }else if(t.getType() == KEYWORD && t.getToken().equalsIgnoreCase("from")) {
                idx = parseFromList(tokens, idx, fromList);
            }else if(t.getType() == KEYWORD && t.getToken().equalsIgnoreCase("where")) {
                where = new Where();
                idx = parseWhere(tokens, idx, where);
            }else if(t.getType() == KEYWORD && t.getToken().equalsIgnoreCase("group")) {
                idx = skipTill(tokens, AFTER_FROM_TOKENS, idx + 1);
            }else if(t.getType() == KEYWORD && t.getToken().equalsIgnoreCase("order")) {
                idx = skipTill(tokens, AFTER_FROM_TOKENS, idx + 1);
            }else if(t.getType() == KEYWORD && t.getToken().equalsIgnoreCase("having")) {
                idx = skipTill(tokens, AFTER_FROM_TOKENS, idx + 1);
            }else if(t.getType() == KEYWORD && t.getToken().equalsIgnoreCase("limit")) {
                idx = skipTill(tokens, AFTER_FROM_TOKENS, idx + 1);
            } else {
                throw new SQLParseException(String.format("Cannot parse '%s'", t.getToken()));
            }
        }
        return SQLSelectQuery.builder()
                .selectFields(selectFieldList)
                .fromTableList(fromList)
                .where(where)
                .build();
    }

    protected int parseWhere(List<Token> tokens, int idx, Where where) {
        idx++;
        int startExpr = idx;
        idx = skipTill(tokens, AFTER_FROM_TOKENS, idx);
        String whereExpression = createTokenString(tokens, startExpr, idx);
        where.setExpression(whereExpression.trim());
        return idx;
    }

    protected int parseSelectList(List<SQLTokenizer.Token> tokens, int idx, List<SelectField> selectFields) throws SQLParseException {
        SQLTokenizer.Token t = tokens.get(idx);
        if (!(t.getType() == KEYWORD && t.getToken().equalsIgnoreCase("select"))) {
            throw new SQLParseException("'select' expected");
        }
        idx++;
        idx = skipSpaces(tokens, idx);
        t = tokens.get(idx);
        boolean distinct = false;
        if (t.getToken().equalsIgnoreCase("distinct")){
            distinct = true;
            idx++;
            idx = skipSpaces(tokens, idx);
        }
        int startSelectItem = idx;
        String alias = null;

        wb:
        while(idx < tokens.size()) {
            t = tokens.get(idx);
            switch (t.getType()) {
                case OPENBRACE:
                    idx = skipBraces(tokens, idx);
                    break;
                case SPACE:
                    idx = skipSpaces(tokens, idx);
                    break;
                case COMMA:
                    SelectField selectField = createSelectField(tokens, startSelectItem, idx);
                    idx = skipSpaces(tokens, idx + 1);
                    startSelectItem = idx;
                    selectFields.add(selectField);
                    alias = null;
                    break;
                case KEYWORD:
                    if (t.getToken().equalsIgnoreCase("as") || t.getToken().equalsIgnoreCase("from")) {
                        int endIdx = idx;
                        if (t.getToken().equalsIgnoreCase("as")) {
                            endIdx = skipSpaces(tokens, endIdx + 1);
                            t = tokens.get(endIdx);
                            endIdx = skipSpaces(tokens, endIdx + 1);
                            alias = t.getToken();
                        }
                        selectField = createSelectField(tokens, startSelectItem, idx);
                        idx = endIdx;
                        if(alias != null) {
                            selectField.setAlias(alias);
                        }
                        selectFields.add(selectField);
                        alias = null;
                        break wb;
                    } else if(t.getToken().equalsIgnoreCase("distinct")){
                        throw new SQLParseException("'distinct' unexpected");
                    }
                    break;
                default:
                    idx++;
            }
        }
        return idx;
    }

    protected SelectField createSelectField(List<SQLTokenizer.Token> tokens, int startIdx, int idx) {
        SelectField selectField = new SelectField();
        fillAliasExpression(tokens, startIdx, idx, selectField);
        return selectField;
    }

    protected void fillAliasExpression(List<SQLTokenizer.Token> tokens, int startIdx, int idx, AliasExpression aliasExpression) {
        // Check if has alias
        int lastNonSpace = idx - 1;
        while(lastNonSpace > startIdx && tokens.get(lastNonSpace).getType() == SPACE) {
            lastNonSpace--;
        }
        String alias = null;
        if(lastNonSpace - startIdx >= 2) {
            Token aliasToken = tokens.get(lastNonSpace);
            Token spaceToken = tokens.get(lastNonSpace - 1);
            Token lastExprToken = tokens.get(lastNonSpace - 2);
            if(aliasToken.getType() == ID &&
                    spaceToken.getType() == SPACE &&
                    (lastExprToken.getType() == ID ||
                            lastExprToken.getType() == STRING ||
                            lastExprToken.getType() == NUMBER ||
                            lastExprToken.getType() == CLOSEBRACE ||
                            lastExprToken.getType() == KEYWORD
                    )
                    ) {
                alias = aliasToken.getToken();
                idx = lastNonSpace - 1;
            }
        }
        StringBuilder expr = new StringBuilder();
        for(int i = startIdx; i < idx; i++) {
            SQLTokenizer.Token t = tokens.get(i);
            expr.append(t.getToken());
        }
        aliasExpression.setExpression(expr.toString().trim());
        aliasExpression.setAlias(alias);
    }


    protected int parseFromList(List<SQLTokenizer.Token> tokens, int idx, List<FromTable> fromTableList) throws SQLParseException {
        SQLTokenizer.Token t = tokens.get(idx);
        if (!(t.getType() == KEYWORD && t.getToken().equalsIgnoreCase("from"))) {
            throw new SQLParseException("'from' expected");
        }
        idx++;
        idx = skipSpaces(tokens, idx);
        t = tokens.get(idx);
        int startFromItem = idx;
        wb:
        while(idx < tokens.size()) {
            t = tokens.get(idx);
            switch (t.getType()) {
                case OPENBRACE:
                    idx = skipBraces(tokens, idx);
                    break;
                case SPACE:
                    idx = skipSpaces(tokens, idx);
                    break;
                case COMMA:
                case KEYWORD:
                    if (
                            t.getToken().equalsIgnoreCase("join") ||
                                    t.getToken().equalsIgnoreCase("left") ||
                                    t.getToken().equalsIgnoreCase("right") ||
                                    t.getToken().equalsIgnoreCase("inner") ||
                                    t.getToken().equalsIgnoreCase("outer") ||
                                    t.getToken().equalsIgnoreCase("as") ||
                                    t.getToken().equalsIgnoreCase("on") ||
                                    t.getToken().equalsIgnoreCase(",")
                            ) {
                        FromTable fromTable = createFromTable(tokens, startFromItem, idx);
                        fromTableList.add(fromTable);
                        if(t.getToken().equalsIgnoreCase("as")) {
                            idx = skipSpaces(tokens, idx + 1);
                            SQLTokenizer.Token tokenAlias = tokens.get(idx);
                            fromTable.setAlias(tokenAlias.getToken());
                            idx = skipSpaces(tokens, idx + 1);
                        }
                        if(idx < tokens.size()) {
                            idx = skipSpaces(tokens, idx);
                            t = tokens.get(idx);
                            if (t.getToken().equalsIgnoreCase("on")) {
                                idx++;
                                startFromItem = idx;
                                idx = skipTill(tokens, JOIN_TOKENS_WITH_WHERE_GROUP_ORDER, idx);

                                String onExpression = createTokenString(tokens, startFromItem, idx).trim();
                                fromTable.setOnExpression(onExpression);

                            }
                            idx = skip(tokens, JOIN_TOKENS_WITHSPACE, idx);
                        }

                        startFromItem = idx;

                    } else if (t.getToken().equalsIgnoreCase("where")) {
                        if(startFromItem < idx) {
                            FromTable fromTable = createFromTable(tokens, startFromItem, idx);
                            fromTableList.add(fromTable);
                        }
                        break wb;
                    } else {
                        idx++;
                    }
                    break;
                default:
                    idx++;
                    if(idx >= tokens.size()) {
                        FromTable fromTable = createFromTable(tokens, startFromItem, idx);
                        fromTableList.add(fromTable);
                    }

            }
        }

        return idx;
    }


    protected int skipTill(List<SQLTokenizer.Token> tokens, List<SQLTokenizer.Token> findTokens, int idx) {
        while(idx < tokens.size()) {
            SQLTokenizer.Token t = tokens.get(idx);
            if(t.getType() == OPENBRACE) {
                idx = skipBraces(tokens, idx);
            } else {

                boolean found = findTokens.stream().anyMatch(st -> {
                    if (st.getToken() != null) {
                        if (st.getToken().equalsIgnoreCase(t.getToken())) {
                            return st.getType() == null || st.getType() == t.getType();
                        }
                        return false;
                    } else {
                        return st.getType() == t.getType();
                    }
                });
                if(!found) {
                    idx++;
                } else {
                    break;
                }
            }

        }
        return idx;

    }

    protected int skip(List<SQLTokenizer.Token> tokens, List<SQLTokenizer.Token> skipTokens, int idx) {
        while(idx < tokens.size()) {
            SQLTokenizer.Token t = tokens.get(idx);
            boolean skip = skipTokens.stream().anyMatch(st -> {
                if (st.getToken() != null) {
                    if (st.getToken().equalsIgnoreCase(t.getToken())) {
                        return st.getType() == null || st.getType() == t.getType();
                    }
                    return false;
                } else {
                    return st.getType() == t.getType();
                }
            });
            if(skip) {
                idx++;
            } else {
                break;
            }
        }
        return idx;
    }

    private FromTable createFromTable(List<SQLTokenizer.Token> tokens, int startIdx, int idx) {
        FromTable fromTable = new FromTable();
        fillAliasExpression(tokens, startIdx, idx, fromTable);
        return fromTable;
    }

    private String createTokenString(List<SQLTokenizer.Token> tokens, int startIdx, int idx) {
        StringBuilder expr = new StringBuilder();
        for(int i = startIdx; i < idx; i++) {
            SQLTokenizer.Token t = tokens.get(i);
            expr.append(t.getToken());
        }
        return expr.toString();
    }
}
