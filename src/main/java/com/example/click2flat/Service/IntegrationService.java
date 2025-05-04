package com.example.click2flat.Service;

import com.example.click2flat.Dao.ClickHouseConfig;
import com.example.click2flat.Dao.ColumnMetaData;
import com.example.click2flat.Dao.FlatFileConfig;
import com.example.click2flat.Dao.IngestionRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class IntegrationService {
    private final ClickHouseService clickHouseService;
    private final FlatFileService flatFileService;



    private int ingestFromFlatFileToClickHouse(IngestionRequest request) throws SQLException, IOException {
        log.info("Ingesting data from Flat File to ClickHouse");

        // Connect to ClickHouse
        try (Connection connection = clickHouseService.connect(request.getClickHouseConfig())) {
            // Create target table in ClickHouse if it doesn't exist
            clickHouseService.createTable(connection, request.getTargetTableName(), request.getSelectedColumns());

            // Read data from flat file
            List<Map<String, Object>> data = flatFileService.readData(
                    request.getFlatFileConfig(),
                    request.getSelectedColumns(),
                    0); // No limit for full ingestion

            // Insert data into ClickHouse
            int recordCount = clickHouseService.insertData(
                    connection,
                    request.getTargetTableName(),
                    request.getSelectedColumns(),
                    data);

            log.info("Ingestion completed: {} records transferred from Flat File to ClickHouse", recordCount);
            return recordCount;
        }
    }

    private int ingestFromClickHouseToFlatFile(IngestionRequest request) throws SQLException, IOException {
        log.info("Ingesting data from ClickHouse to Flat File");

        // Connect to ClickHouse
        try (Connection connection = clickHouseService.connect(request.getClickHouseConfig())) {
            // Create a data handler for writing to flat file
            ClickHouseService.DataHandler flatFileHandler =
                    flatFileService.createFlatFileDataHandler(request.getFlatFileConfig(), request.getSelectedColumns());

            // Transfer data
            int recordCount;
            if (request.getAdditionalTables() != null && !request.getAdditionalTables().isEmpty() &&
                    request.getJoinCondition() != null && !request.getJoinCondition().isEmpty()) {
                // Use JOIN query if multiple tables are selected
                recordCount = clickHouseService.transferJoinDataFromClickHouse(
                        connection,
                        request.getTableName(),
                        request.getAdditionalTables(),
                        request.getJoinCondition(),
                        request.getSelectedColumns(),
                        flatFileHandler);
            } else {
                // Use simple query for single table
                recordCount = clickHouseService.transferDataFromClickHouse(
                        connection,
                        request.getTableName(),
                        request.getSelectedColumns(),
                        flatFileHandler);
            }

            log.info("Ingestion completed: {} records transferred from ClickHouse to Flat File", recordCount);
            return recordCount;
        }
    }




    public int executeIngestion(IngestionRequest request) throws Exception {
        log.info("Starting ingestion from {} to {}", request.getSourceType(), request.getTargetType());

        // Validate request
        validateRequest(request);

        // Execute ingestion based on source and target types
        if ("clickhouse".equalsIgnoreCase(request.getSourceType()) &&
                "flatfile".equalsIgnoreCase(request.getTargetType())) {
            return ingestFromClickHouseToFlatFile(request);
        } else if ("flatfile".equalsIgnoreCase(request.getSourceType()) &&
                "clickhouse".equalsIgnoreCase(request.getTargetType())) {
            return ingestFromFlatFileToClickHouse(request);
        } else {
            throw new IllegalArgumentException("Unsupported source or target type");
        }
    }


    private void validateRequest(IngestionRequest request) {
        if (request.getSourceType() == null || request.getTargetType() == null) {
            throw new IllegalArgumentException("Source and target types must be specified");
        }

        if ("clickhouse".equalsIgnoreCase(request.getSourceType()) && request.getClickHouseConfig() == null) {
            throw new IllegalArgumentException("ClickHouse configuration must be provided when ClickHouse is the source");
        }

        if ("flatfile".equalsIgnoreCase(request.getSourceType()) && request.getFlatFileConfig() == null) {
            throw new IllegalArgumentException("Flat File configuration must be provided when Flat File is the source");
        }

        if ("clickhouse".equalsIgnoreCase(request.getTargetType()) && request.getClickHouseConfig() == null) {
            throw new IllegalArgumentException("ClickHouse configuration must be provided when ClickHouse is the target");
        }

        if ("flatfile".equalsIgnoreCase(request.getTargetType()) && request.getFlatFileConfig() == null) {
            throw new IllegalArgumentException("Flat File configuration must be provided when Flat File is the target");
        }

        if (request.getSelectedColumns() == null || request.getSelectedColumns().isEmpty()) {
            throw new IllegalArgumentException("At least one column must be selected for ingestion");
        }
    }


    public List<Map<String,Object>> previewFlatFileData(FlatFileConfig config, List<ColumnMetaData> columns, int limit) throws IOException {
        return flatFileService.readData(config, columns, limit);
    }


    public List<Map<String, Object>> previewClickHouseJoinData(ClickHouseConfig config, String mainTable,
                                                               List<String> additionalTables, String joinCondition,
                                                               List<ColumnMetaData> columns, int limit) throws SQLException {
        try (Connection connection = clickHouseService.connect(config)) {
            return clickHouseService.previewJoinData(connection, mainTable, additionalTables, joinCondition, columns, limit);
        }
    }


    public List<Map<String, Object>> previewClickHouseData(ClickHouseConfig config, String tableName,
                                                           List<ColumnMetaData> columns, int limit) throws SQLException {
        try (Connection connection = clickHouseService.connect(config)) {
            return clickHouseService.previewData(connection, tableName, columns, limit);
        }
    }




    public List<String> getClickHouseTables(ClickHouseConfig config) throws SQLException {
        try(Connection connection = clickHouseService.connect(config)){
            return clickHouseService.getTables(connection);
        }
    }


    public List<ColumnMetaData> getClickHouseTableSchema(ClickHouseConfig config, String tableName) throws SQLException {
        try(Connection connection = clickHouseService.connect(config)){
            return clickHouseService.getTableSchema(connection, tableName);
        }
    }


    public List<ColumnMetaData> getFlatFileSchema(FlatFileConfig config) throws IOException {
        return flatFileService.readFileSchema(config);
    }


}
