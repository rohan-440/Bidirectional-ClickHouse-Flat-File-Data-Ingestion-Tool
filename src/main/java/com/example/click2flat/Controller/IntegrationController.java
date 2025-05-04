package com.example.click2flat.Controller;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.example.click2flat.Dao.ColumnMetaData;
import com.example.click2flat.Dao.FlatFileConfig;
import com.example.click2flat.Dao.IngestionRequest;
import com.example.click2flat.Service.ClickHouseService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.example.click2flat.Dao.ClickHouseConfig;
import com.example.click2flat.Service.IntegrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

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
    public ResponseEntity<Map<String,Object>> getFlatfileSchema(@RequestBody FlatFileConfig config) {
        Map<String,Object> response = new HashMap<>();

        try {
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
        public ResponseEntity<Map<String,Object>> previewFlatFileData(@RequestBody IngestionRequest request){
            Map<String,Object> response = new HashMap<>();

            try {
                log.info("Received preview request for flat file : {}",request.getFlatFileConfig());
                log.info("Selected columns : {}",request.getSelectedColumns());

                List<Map<String, Object>> data = integrationService.previewFlatFileData(
                        request.getFlatFileConfig(),
                        request.getSelectedColumns(),
                        100
                );

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
    public ResponseEntity<Map<String, Object>> executeIngestion(@RequestBody IngestionRequest request) {
        Map<String, Object> response = new HashMap<>();
        try {
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
