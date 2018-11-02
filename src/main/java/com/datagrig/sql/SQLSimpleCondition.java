package com.datagrig.sql;

import lombok.*;
import rx.annotations.Beta;

/**
 * Condition like
 * a=b
 * t1.a = t2.b
 * t1.a = ?
 * t1.a = 5
 * etc.
 * Created by ukman on 11/2/18.
 */
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class SQLSimpleCondition {
    private String left;
    private String right;
}
