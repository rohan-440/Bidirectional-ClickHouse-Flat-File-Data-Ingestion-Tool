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

//    public String getSourceType() {
//        return sourceType;
//    }
//
//    public void setSourceType(String sourceType) {
//        this.sourceType = sourceType;
//    }
//
//    public String getTargetType() {
//        return targetType;
//    }
//
//    public void setTargetType(String targetType) {
//        this.targetType = targetType;
//    }
//
//    public ClickHouseConfig getClickHouseConfig() {
//        return clickHouseConfig;
//    }
//
//    public void setClickHouseConfig(ClickHouseConfig clickHouseConfig) {
//        this.clickHouseConfig = clickHouseConfig;
//    }
//
//    public FlatFileConfig getFlatFileConfig() {
//        return flatFileConfig;
//    }
//
//    public void setFlatFileConfig(FlatFileConfig flatFileConfig) {
//        this.flatFileConfig = flatFileConfig;
//    }
//
//    public String getTableName() {
//        return tableName;
//    }
//
//    public void setTableName(String tableName) {
//        this.tableName = tableName;
//    }
//
//    public List<String> getAdditionalTables() {
//        return additionalTables;
//    }
//
//    public void setAdditionalTables(List<String> additionalTables) {
//        this.additionalTables = additionalTables;
//    }
//
//    public String getJoinCondition() {
//        return joinCondition;
//    }
//
//    public void setJoinCondition(String joinCondition) {
//        this.joinCondition = joinCondition;
//    }
//
//    public List<ColumnMetaData> getSelectedColumns() {
//        return selectedColumns;
//    }
//
//    public void setSelectedColumns(List<ColumnMetaData> selectedColumns) {
//        this.selectedColumns = selectedColumns;
//    }
//
//    public String getTargetTableName() {
//        return targetTableName;
//    }
//
//    public void setTargetTableName(String targetTableName) {
//        this.targetTableName = targetTableName;
//    }
}
