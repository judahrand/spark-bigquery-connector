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

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.testing.MockGrpcService;
import com.google.api.gax.grpc.testing.MockServiceHelper;
import com.google.cloud.bigquery.storage.v1.BigQueryReadClient;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.MockBigQueryRead;
import com.google.cloud.bigquery.storage.v1.MockBigQueryWrite;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

final class OpenTelemetryTestUtils implements AutoCloseable {

  private final RecordingSpanExporter exporter = new RecordingSpanExporter();
  private final SdkTracerProvider tracerProvider =
      SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(exporter)).build();

  static OpenTelemetryTestUtils install() {
    GlobalOpenTelemetry.resetForTest();
    OpenTelemetryTestUtils telemetry = new OpenTelemetryTestUtils();
    GlobalOpenTelemetry.set(
        OpenTelemetrySdk.builder().setTracerProvider(telemetry.tracerProvider).build());
    return telemetry;
  }

  List<SpanData> getFinishedSpans() {
    return exporter.getFinishedSpans();
  }

  static void executeStorageOperations(BigQueryClientFactory clientFactory) throws IOException {
    MockBigQueryRead mockRead = new MockBigQueryRead();
    MockBigQueryWrite mockWrite = new MockBigQueryWrite();
    MockServiceHelper serviceHelper =
        new MockServiceHelper(
            "opentelemetry-storage-test", Arrays.<MockGrpcService>asList(mockRead, mockWrite));
    serviceHelper.start();
    mockRead.addResponse(ReadSession.getDefaultInstance());
    mockWrite.addResponse(WriteStream.getDefaultInstance());

    BigQueryReadSettings connectorReadSettings =
        clientFactory.getBigQueryReadClient().getSettings();
    BigQueryReadSettings.Builder localReadSettings =
        connectorReadSettings
            .toBuilder()
            .setTransportChannelProvider(serviceHelper.createChannelProvider())
            .setCredentialsProvider(NoCredentialsProvider.create());
    if (connectorReadSettings.isOpenTelemetryEnabled()) {
      localReadSettings
          .setEnableOpenTelemetryTracing(true)
          .setOpenTelemetryTracerProvider(connectorReadSettings.getOpenTelemetryTracerProvider());
    }
    BigQueryReadSettings readSettings = localReadSettings.build();
    BigQueryWriteSettings writeSettings =
        clientFactory
            .getBigQueryWriteClient()
            .getSettings()
            .toBuilder()
            .setTransportChannelProvider(serviceHelper.createChannelProvider())
            .setCredentialsProvider(NoCredentialsProvider.create())
            .build();

    try (BigQueryReadClient readClient = BigQueryReadClient.create(readSettings);
        BigQueryWriteClient writeClient = BigQueryWriteClient.create(writeSettings)) {
      readClient.createReadSession(CreateReadSessionRequest.getDefaultInstance());
      writeClient.createWriteStream(CreateWriteStreamRequest.getDefaultInstance());
    } finally {
      serviceHelper.stop();
    }
  }

  @Override
  public void close() {
    tracerProvider.shutdown().join(10, TimeUnit.SECONDS);
    GlobalOpenTelemetry.resetForTest();
  }

  private static final class RecordingSpanExporter implements SpanExporter {
    private final List<SpanData> finishedSpans = Collections.synchronizedList(new ArrayList<>());

    @Override
    public CompletableResultCode export(Collection<SpanData> spans) {
      finishedSpans.addAll(spans);
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode flush() {
      return CompletableResultCode.ofSuccess();
    }

    @Override
    public CompletableResultCode shutdown() {
      return CompletableResultCode.ofSuccess();
    }

    List<SpanData> getFinishedSpans() {
      synchronized (finishedSpans) {
        return new ArrayList<>(finishedSpans);
      }
    }
  }
}
