package com.example.click2flat.Dao;

import lombok.Data;
import org.springframework.web.multipart.MultipartFile;

@Data
public class FlatFileConfig {

    private MultipartFile fileName; // user will upload
    private String delimiter;
    private boolean hasHeader;
    private String encoding = "UTF-8";


}
