package com.datagrig.sql;

import lombok.*;

/**
 * Created by ukman on 11/1/18.
 */
@Data
@Builder
@ToString(of = "expression")
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class SelectField implements AliasExpression {
    private String name;
    private String alias;
    private String expression;
    private String table;
}
