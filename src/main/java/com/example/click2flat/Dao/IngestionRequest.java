package com.example.click2flat.Dao;

import lombok.Data;

import java.util.List;

@Data
public class IngestionRequest {
    private String sourceType;
    private String targetType;
    private ClickHouseConfig clickHouseConfig;
    private FlatFileConfig flatFileConfig;
    private String tableName;

    //joining purpose
    private List<String> additionalTables;
    private String joinCondition;

    private List<ColumnMetaData> selectedColumns;

    private String targetTableName;


}
