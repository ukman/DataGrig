package com.datagrig.sql;

import lombok.*;

/**
 * Created by ukman on 11/1/18.
 */
@Builder
@EqualsAndHashCode
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Where {
    private String expression;
}
