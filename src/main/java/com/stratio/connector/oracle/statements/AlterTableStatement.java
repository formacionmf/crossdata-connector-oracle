package com.stratio.connector.oracle.statements;

import com.stratio.crossdata.common.data.AlterOperation;
import com.stratio.crossdata.common.data.ColumnName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.metadata.ColumnType;
import com.stratio.crossdata.common.statements.structures.Selector;
import com.stratio.crossdata.common.utils.StringUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by carlos on 22/12/14.
 */
public class AlterTableStatement {

    /**
     * The target table.
     */
    private TableName tableName;

    /**
     * Type of alter.
     */
    private AlterOperation option;

    /**
     * Target column name.
     */
    private ColumnName column;
    /**
     * The list of properties of the table.
     */
    private int defaultLength;
    /**
     * The list of properties of the table.
     */
    private Map<String, Object> properties;

    /**
     * Target column datatype used with {@code ALTER} or {@code ADD}.
     */
    private ColumnType type;

    private static final Map<String, String> columnConvert;
    static {
        Map<String, String> aMap = new HashMap<String,String>();
        aMap.put("text", "varchar2");
        aMap.put("bigint","number");
        aMap.put("double","binary_double");
        aMap.put("float","binary_float");
        aMap.put("boolean","char");
        columnConvert = Collections.unmodifiableMap(aMap);
    }


    /**
     * Class constructor.
     *
     * @param tableName  The name of the table.
     * @param column     The name of the column.
     * @param type       The data type of the column.
     * @param option     The map of options.
     */
    public AlterTableStatement(TableName tableName, ColumnName column, ColumnType type,
                               AlterOperation option,
                               int defaultLength) {

        this.tableName = tableName;
        this.column = column;
        this.type = type;
        this.option = option;
        //this.properties = textOptions;
        //No permite options de momento
        this.properties = new HashMap<String, Object>();
        this.defaultLength = defaultLength;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        sb.append(tableName.getQualifiedName());
        switch (option) {
            case ALTER_COLUMN:
                sb.append(" MODIFY (").append(column.getName());
                sb.append(" ");
                if(columnConvert.get(type.toString().toLowerCase())!=null) {
                    String valColumnConvert = columnConvert.get(type.toString().toLowerCase());
                    sb.append(valColumnConvert);
                    /* any conversion*/
                    if (valColumnConvert.equals("varchar2")) {
                        if (properties.containsKey(column.getName())) {
                            sb.append("(").append(properties.get(column.getName())).append(" char)");
                        } else {
                            sb.append("(").append(defaultLength).append(" char)");
                        }
                    }
                }else {
                    sb.append(type);
                }
                sb.append(")");
                break;
            case ADD_COLUMN:
                sb.append(" ADD ");
                sb.append(column.getName()).append(" ");
                sb.append(type);
                break;
            case DROP_COLUMN:
                sb.append(" DROP COLUMN ");
                sb.append(column.getName());
                break;
            default:
                sb.append("BAD OPTION");
                break;
        }

        return sb.toString();
    }
}
