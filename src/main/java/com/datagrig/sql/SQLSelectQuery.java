package com.datagrig.sql;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * Created by ukman on 11/1/18.
 */
@Data
@Builder
@EqualsAndHashCode(callSuper = false)
public class SQLSelectQuery extends SQLQuery {
    private List<SelectField> selectFields;
    private List<FromTable> fromTableList;
    private Where where;
}
