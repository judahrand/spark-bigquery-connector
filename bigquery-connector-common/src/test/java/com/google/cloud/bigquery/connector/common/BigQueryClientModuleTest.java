/*
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.bigquery.connector.common;

import static com.google.common.truth.Truth.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.gax.retrying.RetrySettings;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.api.services.bigquery.Bigquery;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.QueryJobConfiguration.Priority;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.sun.net.httpserver.HttpServer;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Test;

public class BigQueryClientModuleTest {

  @After
  public void resetGlobalOpenTelemetry() {
    GlobalOpenTelemetry.resetForTest();
  }

  @Test
  public void enabledHttpClientUsesGlobalOpenTelemetryTracer() throws Exception {
    Tracer tracer = mock(Tracer.class);
    TracerProvider tracerProvider = mock(TracerProvider.class);
    when(tracerProvider.get("com.google.cloud.bigquery")).thenReturn(tracer);
    OpenTelemetry openTelemetry = mock(OpenTelemetry.class);
    when(openTelemetry.getTracerProvider()).thenReturn(tracerProvider);
    GlobalOpenTelemetry.set(openTelemetry);

    BigQueryConfig config = createConfig(true);
    BigQueryClient client = createClient(config);
    BigQueryOptions options = getOptions(client);

    assertThat(options.isOpenTelemetryTracingEnabled()).isTrue();
    assertThat(options.getOpenTelemetryTracer()).isSameInstanceAs(tracer);
  }

  @Test
  public void enabledRestMetadataClientUsesTracingInitializer() throws Exception {
    try (OpenTelemetryTestUtils telemetry = OpenTelemetryTestUtils.install()) {
      executeRestTableRequest(createClient(createConfig(true)));

      assertThat(telemetry.getFinishedSpans()).hasSize(1);
      SpanData span = telemetry.getFinishedSpans().get(0);
      assertThat(span.getAttributes().get(AttributeKey.stringKey("http.request.method")))
          .isEqualTo("GET");
      assertThat(span.getAttributes().get(AttributeKey.stringKey("server.address")))
          .isEqualTo("bigquery.googleapis.com");
    }
  }

  @Test
  public void disabledClientsRetainUntracedBehavior() throws Exception {
    try (OpenTelemetryTestUtils telemetry = OpenTelemetryTestUtils.install()) {
      BigQueryClient client = createClient(createConfig(false));
      BigQueryOptions options = getOptions(client);

      assertThat(options.isOpenTelemetryTracingEnabled()).isFalse();
      assertThat(options.getOpenTelemetryTracer()).isNull();

      executeRestTableRequest(client);

      assertThat(telemetry.getFinishedSpans()).isEmpty();
    }
  }

  @Test
  public void driverAndSerializedExecutorFactoryUseTheirJvmGlobals() throws Exception {
    byte[] serializedExecutorFactory;
    try (OpenTelemetryTestUtils driverTelemetry = OpenTelemetryTestUtils.install()) {
      HttpServer server = startBigQueryHttpServer();
      try {
        String endpoint = "http://localhost:" + server.getAddress().getPort();
        BigQueryClient driverClient = createClient(createConfig(true, Optional.of(endpoint)));

        driverClient.getTable(TableId.of("test-project", "test-dataset", "test-table"));
        serializedExecutorFactory = serialize(createSerializableStorageFactory());

        assertThat(spanNames(driverTelemetry))
            .contains("com.google.cloud.bigquery.BigQuery.getTable");
      } finally {
        server.stop(0);
      }
    }

    try (OpenTelemetryTestUtils executorTelemetry = OpenTelemetryTestUtils.install()) {
      BigQueryClientFactory executorFactory = deserialize(serializedExecutorFactory);

      OpenTelemetryTestUtils.executeStorageOperations(executorFactory);

      assertThat(spanNames(executorTelemetry))
          .containsAtLeast(
              "com.google.cloud.bigquery.storage.v1.read.createReadSession",
              "google.cloud.bigquery.storage.v1.BigQueryWrite/CreateWriteStream");
    }
  }

  private static BigQueryClient createClient(BigQueryConfig config) {
    BigQueryCredentialsSupplier credentialsSupplier = mock(BigQueryCredentialsSupplier.class);
    when(credentialsSupplier.getCredentials())
        .thenReturn(
            GoogleCredentials.create(
                new com.google.auth.oauth2.AccessToken("test-token", new Date(Long.MAX_VALUE))));
    when(credentialsSupplier.getUniverseDomain()).thenReturn("googleapis.com");
    EnvironmentContext environmentContext = mock(EnvironmentContext.class);
    when(environmentContext.getBigQueryJobLabels()).thenReturn(ImmutableMap.of());
    Cache<String, TableInfo> destinationTableCache = CacheBuilder.newBuilder().build();

    return new BigQueryClientModule()
        .provideBigQueryClient(
            config,
            FixedHeaderProvider.create("user-agent", "test-agent"),
            credentialsSupplier,
            destinationTableCache,
            environmentContext,
            mock(BigQueryJobCompletionListener.class));
  }

  private static BigQueryConfig createConfig(boolean enableOpenTelemetryTracing) {
    return createConfig(enableOpenTelemetryTracing, Optional.empty());
  }

  private static BigQueryConfig createConfig(
      boolean enableOpenTelemetryTracing, Optional<String> bigQueryHttpEndpoint) {
    BigQueryConfig config = mock(BigQueryConfig.class);
    BigQueryProxyConfig proxyConfig = mock(BigQueryProxyConfig.class);
    when(proxyConfig.getProxyUri()).thenReturn(Optional.empty());
    when(config.getBigQueryProxyConfig()).thenReturn(proxyConfig);
    when(config.getCatalogProjectId()).thenReturn(Optional.of("test-project"));
    when(config.getCatalogLocation()).thenReturn(Optional.empty());
    when(config.getBigQueryHttpEndpoint()).thenReturn(bigQueryHttpEndpoint);
    when(config.getBigQueryClientRetrySettings()).thenReturn(RetrySettings.newBuilder().build());
    when(config.getMaterializationProject()).thenReturn(Optional.empty());
    when(config.getMaterializationDataset()).thenReturn(Optional.empty());
    when(config.getBigQueryJobLabels()).thenReturn(ImmutableMap.of());
    when(config.getQueryJobPriority()).thenReturn(Priority.INTERACTIVE);
    when(config.isOpenTelemetryTracingEnabled()).thenReturn(enableOpenTelemetryTracing);
    return config;
  }

  private static BigQueryClientFactory createSerializableStorageFactory() {
    BigQueryConfig config = mock(BigQueryConfig.class, withSettings().serializable());
    when(config.getBigQueryProxyConfig()).thenReturn(new SerializableProxyConfig());
    when(config.getChannelPoolSize()).thenReturn(1);
    when(config.isOpenTelemetryTracingEnabled()).thenReturn(true);

    BigQueryCredentialsSupplier credentialsSupplier = mock(BigQueryCredentialsSupplier.class);
    when(credentialsSupplier.getCredentials())
        .thenReturn(
            GoogleCredentials.create(
                new com.google.auth.oauth2.AccessToken("test-token", new Date(Long.MAX_VALUE))));
    when(credentialsSupplier.getUniverseDomain()).thenReturn("googleapis.com");
    HeaderProvider headerProvider = mock(HeaderProvider.class, withSettings().serializable());
    when(headerProvider.getHeaders()).thenReturn(ImmutableMap.of("user-agent", "test-agent"));
    return new BigQueryClientFactory(credentialsSupplier, headerProvider, config);
  }

  private static HttpServer startBigQueryHttpServer() throws Exception {
    byte[] response =
        ("{\"kind\":\"bigquery#table\",\"tableReference\":{"
                + "\"projectId\":\"test-project\",\"datasetId\":\"test-dataset\","
                + "\"tableId\":\"test-table\"},\"type\":\"TABLE\"}")
            .getBytes(StandardCharsets.UTF_8);
    HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext(
        "/",
        exchange -> {
          exchange.getResponseHeaders().add("Content-Type", "application/json");
          exchange.sendResponseHeaders(200, response.length);
          exchange.getResponseBody().write(response);
          exchange.close();
        });
    server.start();
    return server;
  }

  private static byte[] serialize(BigQueryClientFactory factory) throws Exception {
    try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        ObjectOutputStream output = new ObjectOutputStream(bytes)) {
      output.writeObject(factory);
      return bytes.toByteArray();
    }
  }

  private static BigQueryClientFactory deserialize(byte[] bytes) throws Exception {
    try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
      return (BigQueryClientFactory) input.readObject();
    }
  }

  private static List<String> spanNames(OpenTelemetryTestUtils telemetry) {
    return telemetry.getFinishedSpans().stream()
        .map(SpanData::getName)
        .collect(Collectors.toList());
  }

  private static final class SerializableProxyConfig implements BigQueryProxyConfig, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public Optional<URI> getProxyUri() {
      return Optional.empty();
    }

    @Override
    public Optional<String> getProxyUsername() {
      return Optional.empty();
    }

    @Override
    public Optional<String> getProxyPassword() {
      return Optional.empty();
    }
  }

  private static BigQueryOptions getOptions(BigQueryClient client) throws Exception {
    Field bigQueryField = BigQueryClient.class.getDeclaredField("bigQuery");
    bigQueryField.setAccessible(true);
    return ((BigQuery) bigQueryField.get(client)).getOptions();
  }

  private static Bigquery getRestClient(BigQueryClient client) throws Exception {
    Field restClientField = BigQueryClient.class.getDeclaredField("bigqueryRestClient");
    restClientField.setAccessible(true);
    return (Bigquery) restClientField.get(client);
  }

  private static void executeRestTableRequest(BigQueryClient client) throws Exception {
    Bigquery restClient = getRestClient(client);
    MockHttpTransport transport =
        new MockHttpTransport.Builder()
            .setLowLevelHttpResponse(
                new MockLowLevelHttpResponse()
                    .setStatusCode(200)
                    .setContentType("application/json")
                    .setContent("{}"))
            .build();
    Bigquery testRestClient =
        new Bigquery.Builder(
                transport,
                GsonFactory.getDefaultInstance(),
                restClient.getRequestFactory().getInitializer())
            .build();
    Field restClientField = BigQueryClient.class.getDeclaredField("bigqueryRestClient");
    restClientField.setAccessible(true);
    restClientField.set(client, testRestClient);

    client.getRestTable(TableId.of("test-project", "test-dataset", "test-table"));
  }
}
