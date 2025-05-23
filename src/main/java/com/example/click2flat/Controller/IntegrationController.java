package com.example.click2flat.Controller;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.example.click2flat.Dao.ColumnMetaData;
import com.example.click2flat.Dao.FlatFileConfig;
import com.example.click2flat.Dao.IngestionRequest;
import com.example.click2flat.Service.ClickHouseService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.click2flat.Dao.ClickHouseConfig;
import com.example.click2flat.Service.IntegrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/integration")
@RequiredArgsConstructor
@Slf4j
public class IntegrationController {
    private final IntegrationService integrationService;






    @PostMapping("/clickhouse/test-connection")
    public ResponseEntity<Map<String,Object>> testClickHouseConnection(@RequestBody ClickHouseConfig config) {
        Map<String,Object> response = new HashMap<>();

        try {
            integrationService.getClickHouseTables(config);
            response.put("Success",true);
            response.put("message","Connection Successful");
            return ResponseEntity.ok(response);
        }
        catch(Exception e){
            log.error("Error Connecting to ClickHouse",e);
            response.put("Success",false);
            response.put("message","Connection Failed " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }

    @PostMapping("/clickhouse/tables")
    public ResponseEntity<Map<String,Object>> getClickhouseTables(@RequestBody ClickHouseConfig config) {
        Map<String,Object> response = new HashMap<>();

        try {
            List<String> tables = integrationService.getClickHouseTables(config);
            response.put("Success",true);
            response.put("tables",tables);
            return ResponseEntity.ok(response);
        }
        catch(Exception e){
            log.error("Error getting ClickHouse Tables",e);
            response.put("Success",false);
            response.put("message","Failed to get ClickHouse Tables" + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }

    @PostMapping("/clickhouse/schema")
    public ResponseEntity<Map<String,Object>> getClickhouseSchema(@RequestBody ClickHouseConfig config, @RequestParam String tableName) {

        Map<String,Object> response = new HashMap<>();

        try {
            List<ColumnMetaData> columns = integrationService.getClickHouseTableSchema(config,tableName);
            response.put("Success",true);
            response.put("tables",columns);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error getting ClickHouse Schema",e);
            response.put("Success",false);
            response.put("message","Failed to get ClickHouse Schema" + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }


    @PostMapping("/flatfile/schema")
    public ResponseEntity<Map<String,Object>> getFlatfileSchema(@RequestParam("file")MultipartFile file, @RequestParam(value = "delimiter",defaultValue = ",") String delimiter,@RequestParam(value = "hasHeader",defaultValue = "true") boolean hasHeader) {
        Map<String,Object> response = new HashMap<>();

        try {
            FlatFileConfig config = new FlatFileConfig();
            config.setFileName(file);
            config.setDelimiter(delimiter);
            config.setHasHeader(hasHeader);
            List<ColumnMetaData>columns = integrationService.getFlatFileSchema(config);
            response.put("Success",true);
            response.put("columns",columns);
            return ResponseEntity.ok(response);
        }
        catch(Exception e){
            log.error("Error getting FlatFile Schema",e);
            response.put("Success",false);
            response.put("message","Failed to get FlatFile Schema" + e.getMessage());
            return ResponseEntity.badRequest().body(response);

        }
    }


    @PostMapping("/clickhouse/preview")
    public ResponseEntity<Map<String, Object>> previewClickHouseData(@RequestBody IngestionRequest request) {
        Map<String, Object> response = new HashMap<>();
        try {
            List<Map<String, Object>> data;
            if (request.getAdditionalTables() != null && !request.getAdditionalTables().isEmpty() &&
                    request.getJoinCondition() != null && !request.getJoinCondition().isEmpty()) {
                // Use JOIN preview if multiple tables are selected
                data = integrationService.previewClickHouseJoinData(
                        request.getClickHouseConfig(),
                        request.getTableName(),
                        request.getAdditionalTables(),
                        request.getJoinCondition(),
                        request.getSelectedColumns(),
                        100); // Preview limit
            } else {
                // Use simple preview for single table
                data = integrationService.previewClickHouseData(
                        request.getClickHouseConfig(),
                        request.getTableName(),
                        request.getSelectedColumns(),
                        100); // Preview limit
            }
            response.put("success", true);
            response.put("data", data);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error previewing ClickHouse data", e);
            response.put("success", false);
            response.put("message", "Failed to preview data: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }

        @PostMapping("/flatfile/preview")
        public ResponseEntity<Map<String,Object>> previewFlatFileData(@RequestParam("file") MultipartFile file, // Accept uploaded CSV file
                                                                      @RequestParam("delimiter") String delimiter, // Accept delimiter (comma, semicolon, etc.)
                                                                      @RequestParam("hasHeader") boolean hasHeader, // Accept if the CSV has headers
                                                                      @RequestParam("selectedColumns") String selectedColumnsJson // Accept selected columns JSON as string
        ) {
            Map<String, Object> response = new HashMap<>();

            try {
                // Parse the selected columns from the JSON string to List<ColumnMetaData>
                ObjectMapper objectMapper = new ObjectMapper();
                List<ColumnMetaData> selectedColumns = objectMapper.readValue(
                        selectedColumnsJson,
                        new TypeReference<List<ColumnMetaData>>() {}
                );

                // Create FlatFileConfig from the incoming request data
                FlatFileConfig config = new FlatFileConfig();
                config.setFileName(file); // Set the uploaded file
                config.setDelimiter(delimiter); // Set the delimiter
                config.setHasHeader(hasHeader); // Set header presence
                config.setEncoding("UTF-8"); // Default encoding

                // Call your service method to preview the flat file data
                List<Map<String, Object>> data = integrationService.previewFlatFileData(config, selectedColumns, 100);

                response.put("success", true);
                response.put("data", data);
                return ResponseEntity.ok(response);
            }
            catch(Exception e){
                log.error("Error previewing FlatFile data",e);
                response.put("success", false);
                response.put("message", "Failed to preview data: " + e.getMessage());
                return ResponseEntity.badRequest().body(response);
            }
        }
    @PostMapping("/execute")
    public ResponseEntity<Map<String, Object>> executeIngestion(@RequestParam("file") MultipartFile file,
                                                                @RequestParam("request") String requestJson
    ) {
        Map<String, Object> response = new HashMap<>();
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            IngestionRequest request = objectMapper.readValue(requestJson, IngestionRequest.class);

            // Inject the file into FlatFileConfig
            FlatFileConfig config = request.getFlatFileConfig();
            config.setFileName(file);  // assumes FlatFileConfig has MultipartFile file field

            // Now call the service
            int recordCount = integrationService.executeIngestion(request);

            response.put("success", true);
            response.put("recordCount", recordCount);
            response.put("message", "Ingestion completed successfully");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            log.error("Error executing ingestion", e);
            response.put("success", false);
            response.put("message", "Ingestion failed: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);

        }
    }

}
