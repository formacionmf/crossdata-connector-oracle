package com.stratio.connector.oracle.utils;

import com.stratio.crossdata.common.metadata.ColumnType;

/**
 * Created by carlos on 23/12/14.
 */
public class ColumnInsertOracle {
    private ColumnType type;
    private String value;
    private String columnName;

    /**
     * Basic Constructor.
     * @param type Type of the Column.
     * @param value Value of the Column.
     * @param columnName String with the column name.
     */
    public ColumnInsertOracle(ColumnType type, String value, String columnName) {
        this.type = type;
        this.value = value;
        this.columnName = columnName;
    }

    /**
     * Get the type.
     * @return ColumnType
     */
    public ColumnType getType() {
        return type;
    }

    /**
     * Set the type.
     * @param type
     */
    public void setType(ColumnType type) {
        this.type = type;
    }

    /**
     * Get the value.
     * @return String with the value of a Column.
     */
    public String getValue() {
        return value;
    }

    /**
     * Set the value of a column.
     * @param value
     */
    public void setValue(String value) {
        this.value = value;
    }

    /**
     * Get the String with the column name.
     * @return String.
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Set the column name.
     * @param columnName
     */
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }
}
