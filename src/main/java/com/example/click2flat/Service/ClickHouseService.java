package com.example.click2flat.Service;

import com.example.click2flat.Dao.ColumnMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.click2flat.Dao.ClickHouseConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.sql.*;
import java.util.*;

@Service
@Slf4j
public class ClickHouseService {
    public int insertData(Connection connection, String tableName, List<ColumnMetaData> columns,
                          List<Map<String, Object>> data) throws SQLException {
        if (data.isEmpty()) {
            return 0;
        }

        // Build column list for INSERT query
        StringBuilder columnList = new StringBuilder();
        for (ColumnMetaData column : columns) {
            if (column.isSelected()) {
                if (columnList.length() > 0) {
                    columnList.append(", ");
                }
                columnList.append("`").append(column.getName()).append("`");
            }
        }

        // If no columns selected, return 0
        if (columnList.length() == 0) {
            return 0;
        }

        // Prepare batch insert query
        String insertQuery = String.format("INSERT INTO %s (%s) VALUES (?, ?, ?, ?)", tableName, columnList);

        // Get selected column names
        List<String> selectedColumnNames = new ArrayList<>();
        for (ColumnMetaData column : columns) {
            if (column.isSelected()) {
                selectedColumnNames.add(column.getName());
            }
        }

        // Use batch insert for better performance
        try (PreparedStatement pstmt = connection.prepareStatement(insertQuery)) {
            int batchSize = 1000;
            int count = 0;

            for (Map<String, Object> row : data) {
                int paramIndex = 1;
                for (String columnName : selectedColumnNames) {
                    pstmt.setObject(paramIndex++, row.get(columnName));
                }

                pstmt.addBatch();
                count++;

                if (count % batchSize == 0) {
                    pstmt.executeBatch();
                    log.info("Inserted {} records", count);
                }
            }

            // Execute remaining batch
            if (count % batchSize != 0) {
                pstmt.executeBatch();
            }

            return count;
        }
    }

     public void createTable(Connection connection, String tableName, List<ColumnMetaData> columns) throws SQLException {
        StringBuilder createTableQuery = new StringBuilder();
        createTableQuery.append("CREATE TABLE IF NOT EXISTS ").append("Db.").append(tableName).append(" (");

        boolean first = true;
        for (ColumnMetaData column : columns) {
            if (column.isSelected()) {
                if (!first) {
                    createTableQuery.append(", ");
                }
                createTableQuery.append(" ").append(column.getName()).append(" ");

                // Map CSV types to ClickHouse types
                String clickHouseType = mapToClickHouseType(column.getType());
                createTableQuery.append(clickHouseType);

                first = false;
            }
        }

        createTableQuery.append(") ENGINE = MergeTree() ORDER BY ");
        createTableQuery.append(" ").append(columns.get(0).getName()).append(" "); // or another column

        String query = createTableQuery.toString();
        log.info("Creating table with query: {}", query);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(query);
        }
    }


    private String mapToClickHouseType(String genericType) {
        // This is a simplified mapping, can be extended based on requirements
        if (genericType == null || genericType.isEmpty()) {
            return "String";
        }

        String lowerType = genericType.toLowerCase();

        if (lowerType.contains("int")) {
            return "Int64";
        } else if (lowerType.contains("float") || lowerType.contains("double") || lowerType.contains("decimal")) {
            return "Float64";
        } else if (lowerType.contains("date")) {
            return "DateTime";
        } else if (lowerType.contains("bool")) {
            return "UInt8";
        } else {
            return "String";
        }
    }

    public int transferDataFromClickHouse(Connection connection, String tableName,
                                          List<ColumnMetaData> columns, DataHandler handler) throws SQLException {
        int recordCount = 0;

        // Build column list for SELECT query
        StringBuilder columnList = new StringBuilder();
        for (ColumnMetaData column : columns) {
            if (column.isSelected()) {
                if (columnList.length() > 0) {
                    columnList.append(", ");
                }
                columnList.append("`").append(column.getName()).append("`");
            }
        }

        // If no columns selected, return 0
        if (columnList.length() == 0) {
            return 0;
        }

        String query = String.format("SELECT %s FROM %s", columnList, tableName);
        log.info("Executing transfer query: {}", query);

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Process each row
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);
                    row.put(columnName, value);
                }

                handler.processRow(row);
                recordCount++;

                // Log progress every 1000 records
                if (recordCount % 1000 == 0) {
                    log.info("Processed {} records", recordCount);
                }
            }
        }

        handler.complete();
        return recordCount;
    }

    //get the connection clickhouse DB
    public Connection connect(ClickHouseConfig config) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", config.getUser());

        if(config.getJwtToken() != null && !config.getJwtToken().isEmpty()) {
            properties.setProperty("password", config.getJwtToken());
            properties.setProperty("ssl", String.valueOf(config.isSecure()));
            properties.setProperty("use_client_time_zone", "true");
        }
        log.info("Connecting to ClickHouse at {}", config.getJdbcUrl());

        return DriverManager.getConnection(config.getJdbcUrl(), properties);
    }

    //get the tables of clickhouse

    public List<String> getTables(Connection connection) throws SQLException {
        List<String> tables = new ArrayList<>();

        try (Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery("SHOW TABLES")){

            while (rs.next()) {
                tables.add(rs.getString(1));
            }
        }
        return tables;
    }


    //get the Table Schema
    public List<ColumnMetaData> getTableSchema(Connection connection, String tableName) throws SQLException {
        List<ColumnMetaData> columns = new ArrayList<>();
        String query = "DESCRIBE TABLE " + tableName;

        try(Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(query)){
            while (rs.next()) {
                String columnName = rs.getString("name");
                String columnType = rs.getString("type");
                columns.add(new ColumnMetaData(columnName, columnType));
            }
        }
        return columns;
    }



     // get the preview of the data

    public List<Map<String,Object>> previewData(Connection connection, String tableName, List<ColumnMetaData> columns, int limit) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<>();

        // Build column list for SELECT query
        StringBuilder columnList = new StringBuilder();
        for (ColumnMetaData column : columns) {
            if (column.isSelected()) {
                if (columnList.length() > 0) {
                    columnList.append(", ");
                }
                columnList.append("`").append(column.getName()).append("`");
            }
        }

        // If no columns selected, return empty result
        if (columnList.length() == 0) {
            return results;
        }

        String query = String.format("SELECT %s FROM %s LIMIT %d", columnList, tableName, limit);
        log.info("Executing preview query: {}", query);

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);
                    row.put(columnName, value);
                }
                results.add(row);
            }
        }

        return results;
    }



    //get the preview with join Tables

    public List<Map<String,Object>> previewJoinData(Connection connection,String mainTable, List<String> additionalTables, String joinCondition, List<ColumnMetaData> columns, int limit) throws SQLException {
        List<Map<String, Object>> results = new ArrayList<>();

        // Build column list for SELECT query
        StringBuilder columnList = new StringBuilder();
        for (ColumnMetaData column : columns) {
            if (column.isSelected()) {
                if (columnList.length() > 0) {
                    columnList.append(", ");
                }
                columnList.append("`").append(column.getName()).append("`");
            }
        }

        // If no columns selected, return empty result
        if (columnList.length() == 0) {
            return results;
        }

        // Build JOIN query
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SELECT ").append(columnList);
        queryBuilder.append(" FROM ").append(mainTable);

        // Add JOIN clauses
        if (additionalTables != null && !additionalTables.isEmpty() && joinCondition != null && !joinCondition.isEmpty()) {
            for (String table : additionalTables) {
                queryBuilder.append(" JOIN ").append(table);
            }
            queryBuilder.append(" ON ").append(joinCondition);
        }

        queryBuilder.append(" LIMIT ").append(limit);

        String query = queryBuilder.toString();
        log.info("Executing JOIN preview query: {}", query);

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);
                    row.put(columnName, value);
                }
                results.add(row);
            }
        }

        return results;
    }



    public int transferJoinDataFromClickHouse(Connection connection, String mainTable,
                                              List<String> additionalTables, String joinCondition,
                                              List<ColumnMetaData> columns, DataHandler handler) throws SQLException {
        int recordCount = 0;

        // Build column list for SELECT query
        StringBuilder columnList = new StringBuilder();
        for (ColumnMetaData column : columns) {
            if (column.isSelected()) {
                if (columnList.length() > 0) {
                    columnList.append(", ");
                }
                columnList.append("`").append(column.getName()).append("`");
            }
        }

        // If no columns selected, return 0
        if (columnList.length() == 0) {
            return 0;
        }

        // Build JOIN query
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("SELECT ").append(columnList);
        queryBuilder.append(" FROM ").append(mainTable);

        // Add JOIN clauses
        if (additionalTables != null && !additionalTables.isEmpty() && joinCondition != null && !joinCondition.isEmpty()) {
            for (String table : additionalTables) {
                queryBuilder.append(" JOIN ").append(table);
            }
            queryBuilder.append(" ON ").append(joinCondition);
        }

        String query = queryBuilder.toString();
        log.info("Executing JOIN transfer query: {}", query);

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(query)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Process each row
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    Object value = rs.getObject(i);
                    row.put(columnName, value);
                }

                handler.processRow(row);
                recordCount++;

                // Log progress every 1000 records
                if (recordCount % 1000 == 0) {
                    log.info("Processed {} records", recordCount);
                }
            }
        }

        handler.complete();
        return recordCount;
    }




    public interface DataHandler {
        void processRow(Map<String, Object> row) throws SQLException;
        void complete() throws SQLException;
    }
}
