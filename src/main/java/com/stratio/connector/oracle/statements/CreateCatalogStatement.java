package com.stratio.connector.oracle.statements;

/**
 * Created by carlos on 18/12/14.
 */
public class CreateCatalogStatement {
    /**
     * Catalog.
     */
    private String catalog;

    /**
     * Class constructor.
     *
     * @param catalogName The name of the catalog.
     */
    public CreateCatalogStatement(String catalogName) {
        this.catalog = catalogName;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("SELECT * FROM ALL_USERS WHERE USERNAME = '");

        sb.append(catalog.toUpperCase());
        sb.append("'");

        return sb.toString();
    }
}
