package com.datagrig.sql;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ukman on 11/2/18.
 */
public class SQLSimpleConditionParser extends SQLAbstractParser{
    public SQLSimpleCondition parse(String sql) throws SQLParseException, SQLException {
        SQLTokenizer tokenizer = new SQLTokenizer();
        List<SQLTokenizer.Token> tokens = tokenizer.getTokens(sql);
        return parse(tokens);
    }

    public SQLSimpleCondition parse(List<SQLTokenizer.Token> tokens) throws SQLParseException {
        List<SQLTokenizer.Token> preparedTokens = new ArrayList();
        for(SQLTokenizer.Token t : tokens) {
            if(t.getType() != SQLTokenizer.SQLTokenType.SPACE) {
                preparedTokens.add(t);
                if(preparedTokens.size() > 3) {
                    throw new SQLParseException("It's not simple condition");
                }
            }
        }
        if(preparedTokens.size() != 3) {
            throw new SQLParseException("It's not simple condition");
        }
        SQLTokenizer.Token leftToken = preparedTokens.get(0);
        SQLTokenizer.Token eqToken = preparedTokens.get(1);
        if(eqToken.getType() != SQLTokenizer.SQLTokenType.EQ) {
            throw new SQLParseException("It's not simple condition");
        }
        SQLTokenizer.Token rightToken = preparedTokens.get(2);
        return SQLSimpleCondition.builder().left(leftToken.getToken()).right(rightToken.getToken()).build();
    }

}
