package com.example.click2flat.Dao;

import lombok.Data;

@Data
public class ClickHouseConfig {
    private String host;
    private int port;
    private String database;
    private String user;
    private String jwtToken;
    private boolean secure;

    public String getJdbcUrl() {
        String protocol = secure ? "https" : "http";
        // Add compress=0 parameter to disable compression and avoid LZ4 dependency issue
        return String.format("jdbc:clickhouse:%s://%s:%d/%s?compress=0", protocol, host, port, database);
    }

//    public String getHost() {
//        return host;
//    }
//
//    public void setHost(String host) {
//        this.host = host;
//    }
//
//    public int getPort() {
//        return port;
//    }
//
//    public void setPort(int port) {
//        this.port = port;
//    }
//
//    public String getDatabase() {
//        return database;
//    }
//
//    public void setDatabase(String database) {
//        this.database = database;
//    }
//
//    public String getUser() {
//        return user;
//    }
//
//    public void setUser(String user) {
//        this.user = user;
//    }
//
//    public String getJwtToken() {
//        return jwtToken;
//    }
//
//    public void setJwtToken(String jwtToken) {
//        this.jwtToken = jwtToken;
//    }
//
//    public boolean isSecure() {
//        return secure;
//    }
//
//    public void setSecure(boolean secure) {
//        this.secure = secure;
//    }
}
