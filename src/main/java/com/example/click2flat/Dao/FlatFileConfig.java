package com.example.click2flat.Dao;

import lombok.Data;

@Data
public class FlatFileConfig {
    private String fileName;
    private String delimiter;
    private boolean hasHeader;
    private String encoding = "UTF-8";


}
