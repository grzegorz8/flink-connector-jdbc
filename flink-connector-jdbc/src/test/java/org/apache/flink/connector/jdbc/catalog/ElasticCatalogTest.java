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

import org.apache.flink.connector.jdbc.dialect.elastic.ElasticsearchTestContainer;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;

import okhttp3.Credentials;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ElasticCatalogTest {

    private static final ElasticsearchTestContainer container = new ElasticsearchTestContainer();

    private static final String INPUT_SINGLE_RECORD_TABLE = "test_single_record_table";
    private static final String INPUT_SINGLE_EVENT_PATH = "elastic-test-data/test-data.json";
    private static final String INDEX_PATH = "elastic-test-data/test-index.json";

    @BeforeAll
    public static void beforeAll() throws Exception {
        container.withEnv("xpack.security.enabled", "true");
        container.withEnv("ELASTIC_PASSWORD", "password");
        container.start();
        Class.forName(container.getDriverClassName());
        enableTrial();
        createTestIndex(INPUT_SINGLE_RECORD_TABLE, INDEX_PATH);
        addTestData(INPUT_SINGLE_RECORD_TABLE, INPUT_SINGLE_EVENT_PATH);
    }

    private static void enableTrial() throws Exception {
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
            .url(String.format("http://%s:%d/_license/start_trial?acknowledge=true",
                container.getHost(), container.getElasticPort()))
            .post(RequestBody.create(new byte[]{}))
            .addHeader("Authorization", Credentials.basic("elastic", "password"))
            .build();
        try (Response response = client.newCall(request).execute()) {
            assertTrue(response.isSuccessful());
        }
    }

    private static void createTestIndex(String inputTable, String indexPath) throws Exception {
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
            .url(String.format("http://%s:%d/%s/", container.getHost(),
                container.getElasticPort(), inputTable))
            .put(RequestBody.create(loadResource(indexPath)))
            .addHeader("Content-Type", "application/json")
            .addHeader("Authorization", Credentials.basic("elastic", "password"))
            .build();
        try (Response response = client.newCall(request).execute()) {
            assertTrue(response.isSuccessful());
        }
    }

    private static void addTestData(String inputTable, String inputPath) throws Exception {
        OkHttpClient client = new OkHttpClient();
        Request request = new Request.Builder()
            .url(String.format("http://%s:%d/%s/_bulk/", container.getHost(),
                container.getElasticPort(), inputTable))
            .post(RequestBody.create(loadResource(inputPath)))
            .addHeader("Content-Type", "application/json")
            .addHeader("Authorization", Credentials.basic("elastic", "password"))
            .build();
        try (Response response = client.newCall(request).execute()) {
            assertTrue(response.isSuccessful());
        }
    }

    private static byte[] loadResource(String path) throws IOException {
        return IOUtils.toByteArray(
            Objects.requireNonNull(ElasticCatalogTest.class.getClassLoader().getResourceAsStream(path))
        );
    }

    @Test
    public void shouldListDatabases() {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
            container.getElasticPort());

        // when
        ElasticCatalog catalog = new ElasticCatalog(Thread.currentThread().getContextClassLoader(),
            "test-catalog", "test-database", "elastic", "password", url);

        // then
        List<String> databases = catalog.listDatabases();
        assertEquals(1, databases.size());
        assertEquals("docker-cluster", databases.get(0));
    }

    @Test
    public void shouldListTables() throws DatabaseNotExistException {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
            container.getElasticPort());

        // when
        ElasticCatalog catalog = new ElasticCatalog(Thread.currentThread().getContextClassLoader(),
            "test-catalog", "test-database", "elastic", "password", url);

        // then
        List<String> tables = catalog.listTables("docker-cluster");
        List<String> expectedTables = new LinkedList<>();
        expectedTables.add("test_single_record_table");

        assertEquals(1, tables.size());
        assertTrue(tables.containsAll(expectedTables));
    }

    @Test
    public void shouldVerifyTableExists() {
        // given
        String url = String.format("jdbc:elasticsearch://%s:%d", container.getHost(),
            container.getElasticPort());

        // when
        ElasticCatalog catalog = new ElasticCatalog(Thread.currentThread().getContextClassLoader(),
            "test-catalog", "test-database", "elastic", "password", url);
        catalog.open();

        // then
        assertTrue(catalog.tableExists(new ObjectPath("docker-cluster", "test_single_record_table")));
    }

}
