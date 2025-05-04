package com.example.click2flat.Dao;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ColumnMetaData {

    private  String name;
    private String type;
    private boolean selected;

    public ColumnMetaData(String name, String type) {
        this.name = name;
        this.type = type;
        this.selected = false;
    }
}
