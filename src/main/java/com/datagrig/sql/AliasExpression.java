package com.datagrig.sql;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by ukman on 11/1/18.
 */
public interface AliasExpression {
    String getAlias();
    void setAlias(String alias);
    String getExpression();
    void setExpression(String expression);
}
