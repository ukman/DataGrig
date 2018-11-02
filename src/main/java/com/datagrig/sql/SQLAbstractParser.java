package com.datagrig.sql;

import java.util.List;

import static com.datagrig.sql.SQLTokenizer.SQLTokenType.OPENBRACE;
import static com.datagrig.sql.SQLTokenizer.SQLTokenType.SPACE;

/**
 * Created by ukman on 11/2/18.
 */
public class SQLAbstractParser {

    protected int skipSpaces(List<SQLTokenizer.Token> tokens, int i) {
        while(i < tokens.size() && tokens.get(i).getType() == SPACE) {
            i++;
        }
        if(i >= tokens.size()) {
//            i = tokens.size() - 1;
        }
        return i;
    }

    protected int skipBraces(List<SQLTokenizer.Token> tokens, int i) {
        SQLTokenizer.Token t = tokens.get(i);
        int deep = t.getType() == OPENBRACE ? 1 : 0;
        while(deep > 0) {
            i++;
            t = tokens.get(i);
            switch(t.getType()) {
                case OPENBRACE :
                    deep++;
                    break;
                case CLOSEBRACE :
                    deep--;
                    break;
            }
        }
        return i + 1;
    }
}
