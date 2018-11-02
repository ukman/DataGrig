package com.datagrig.sql;

import lombok.*;

/**
 * Created by ukman on 11/1/18.
 */
@Data
@Builder
@EqualsAndHashCode
@AllArgsConstructor
@NoArgsConstructor
public class FromTable implements AliasExpression{
    // private String table;
    private String expression;
    private String alias;
    private String onExpression;

    public String getAliasOrTableName() {
        if(alias != null) {
            return alias;
        }
        return expression;
    }
}
