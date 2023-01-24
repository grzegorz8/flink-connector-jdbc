/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.catalog;

import org.apache.flink.connector.jdbc.dialect.elastic.ElasticTypeMapper;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.types.DataType;

import org.apache.commons.compress.utils.Lists;
import org.elasticsearch.xpack.sql.jdbc.EsDriver;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactoryOptions.IDENTIFIER;
import static org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactoryOptions.PASSWORD;
import static org.apache.flink.connector.jdbc.catalog.factory.JdbcCatalogFactoryOptions.USERNAME;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.TABLE_NAME;
import static org.apache.flink.connector.jdbc.table.JdbcConnectorOptions.URL;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/** Catalog for Elastic. */
public class ElasticCatalog extends AbstractJdbcCatalog {

    static {
        try {
            EsDriver.register();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private final ElasticTypeMapper dialectTypeMapper;

    public ElasticCatalog(
            ClassLoader userClassLoader,
            String catalogName,
            String defaultDatabase,
            String username,
            String password,
            String baseUrl) {
        // In elastic the default database name is not a part of defaultUrl, therefore, pass baseUrl
        // as defaultUrl.
        super(userClassLoader, catalogName, defaultDatabase, username, password, baseUrl, baseUrl);
        this.dialectTypeMapper = new ElasticTypeMapper();
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        List<String> databases = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(baseUrl, username, pwd)) {
            try (Statement statement = connection.createStatement();
                 ResultSet results = statement.executeQuery("SHOW CATALOGS")) {
                while (results.next()) {
                    databases.add(results.getString(1));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return databases;
    }

    @Override
    public List<String> listTables(String databaseName)
            throws DatabaseNotExistException, CatalogException {
        List<String> tables = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(baseUrl, username, pwd)) {
            try (Statement statement = connection.createStatement();
                 ResultSet results = statement.executeQuery("SHOW TABLES CATALOG '" + databaseName + "'")) {
                while (results.next()) {
                    tables.add(results.getString(2));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return tables;
    }

    @Override
    public boolean tableExists(ObjectPath tablePath) throws CatalogException {
        try {
            return databaseExists(tablePath.getDatabaseName())
                && listTables(tablePath.getDatabaseName()).contains(tablePath.getObjectName());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public CatalogBaseTable getTable(ObjectPath tablePath)
        throws TableNotExistException, CatalogException {

        if (!tableExists(tablePath)) {
            throw new TableNotExistException(getName(), tablePath);
        }

        String databaseName = tablePath.getDatabaseName();

        try (Connection conn = DriverManager.getConnection(baseUrl, username, pwd)) {
            DatabaseMetaData metaData = conn.getMetaData();
            Optional<UniqueConstraint> primaryKey =
                getPrimaryKey(
                    metaData,
                    databaseName,
                    getSchemaName(tablePath),
                    getTableName(tablePath));

            ResultSetMetaData resultSetMetaData = retrieveResultSetMetaData(conn, tablePath);

            Map<String, DataType> columns = retrieveColumns(resultSetMetaData, tablePath);
            String[] columnNames = columns.keySet().toArray(new String[0]);
            DataType[] types = columns.values().toArray(new DataType[0]);

            Schema tableSchema = buildSchema(columnNames, types, primaryKey);
            String tableName = getSchemaTableName(tablePath);

            Map<String, String> props = new HashMap<>();
            props.put(CONNECTOR.key(), IDENTIFIER);
            props.put(URL.key(), baseUrl);
            props.put(USERNAME.key(), username);
            props.put(PASSWORD.key(), pwd);
            props.put(TABLE_NAME.key(), tableName);
            return CatalogTable.of(tableSchema, null, Lists.newArrayList(), props);
        } catch (Exception e) {
            throw new CatalogException(format("Failed getting table %s.", tablePath.getFullName()), e);
        }
    }

    private Map<String, DataType> retrieveColumns(ResultSetMetaData resultSetMetaData,
                                                  ObjectPath tablePath) throws SQLException {
        Map<String, DataType> columns = new HashMap<>();

        for (int i = 1; i <= resultSetMetaData.getColumnCount(); i++) {
            String columnName = resultSetMetaData.getColumnName(i);
            DataType type = fromJDBCType(tablePath, resultSetMetaData, i);
            if (resultSetMetaData.isNullable(i) == ResultSetMetaData.columnNoNulls) {
                type = type.notNull();
            }
            columns.put(columnName, type);
        }
        return columns;
    }

    private ResultSetMetaData retrieveResultSetMetaData(Connection conn, ObjectPath tablePath)throws SQLException {
        String query = format("SELECT * FROM \"%s\" LIMIT 0", getSchemaTableName(tablePath));
        PreparedStatement ps = conn.prepareStatement(query);
        ResultSet rs = ps.executeQuery();
        return rs.getMetaData();
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    private Schema buildSchema(String[] columnNames, DataType[] types, Optional<UniqueConstraint> primaryKey) {
        Schema.Builder schemaBuilder = Schema.newBuilder().fromFields(columnNames, types);
        primaryKey.ifPresent(
            pk -> schemaBuilder.primaryKeyNamed(pk.getName(), pk.getColumns()));
        return schemaBuilder.build();
    }

    @Override
    protected DataType fromJDBCType(ObjectPath tablePath, ResultSetMetaData metadata, int colIndex) throws SQLException {
        return dialectTypeMapper.mapping(tablePath, metadata, colIndex);
    }

    @Override
    protected String getSchemaName(ObjectPath tablePath) {
        return tablePath.getDatabaseName();
    }

    @Override
    protected String getTableName(ObjectPath tablePath) {
        return tablePath.getObjectName();
    }

    @Override
    protected String getSchemaTableName(ObjectPath tablePath) {
        return tablePath.getObjectName();
    }
}
