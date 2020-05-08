/*
 * SPDX-License-Identifier: Apache-2.0
 * Copyright 2018-2020 The Feast Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package feast.storage.connectors.snowflake.retriever;

import com.google.auto.value.AutoValue;
import feast.serving.ServingAPIProto;
import feast.serving.ServingAPIProto.DatasetSource;
import feast.storage.api.retriever.FeatureSetRequest;
import feast.storage.api.retriever.HistoricalRetrievalResult;
import feast.storage.api.retriever.HistoricalRetriever;
import io.grpc.Status;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.slf4j.Logger;

@AutoValue
public abstract class SnowflakeHistoricalRetriever implements HistoricalRetriever {

  private static final Logger log =
      org.slf4j.LoggerFactory.getLogger(SnowflakeHistoricalRetriever.class);

  public static final long TEMP_TABLE_EXPIRY_DURATION_MS = Duration.ofDays(1).toMillis();
  private static final long SUBQUERY_TIMEOUT_SECS = 900; // 15 minutes

  public static HistoricalRetriever create(Map<String, String> config) {

    String jobStagingLocation = config.get("staging_location");
    if (!jobStagingLocation.contains("://")) {
      throw new IllegalArgumentException(
          String.format("jobStagingLocation is not a valid URI: %s", jobStagingLocation));
    }
    if (jobStagingLocation.endsWith("/")) {
      jobStagingLocation = jobStagingLocation.substring(0, jobStagingLocation.length() - 1);
    }
    if (!jobStagingLocation.startsWith("gs://")) {
      throw new IllegalArgumentException(
          "Store type BIGQUERY requires job staging location to be a valid and existing Google Cloud Storage URI. Invalid staging location: "
              + jobStagingLocation);
    }

    return builder()
        .setDatasetId(config.get("dataset_id"))
        .setProjectId(config.get("project_id"))
        .setJobStagingLocation(config.get("staging_location"))
        .setInitialRetryDelaySecs(Integer.parseInt(config.get("initial_retry_delay_seconds")))
        .setTotalTimeoutSecs(Integer.parseInt(config.get("total_timeout_seconds")))
        .build();
  }

  public abstract String projectId();

  public abstract String datasetId();

  public abstract String jobStagingLocation();

  public abstract int initialRetryDelaySecs();

  public abstract int totalTimeoutSecs();

  public static Builder builder() {
    return new AutoValue_SnowflakeHistoricalRetriever.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setProjectId(String projectId);

    public abstract Builder setDatasetId(String datasetId);

    public abstract Builder setJobStagingLocation(String jobStagingLocation);

    public abstract Builder setInitialRetryDelaySecs(int initialRetryDelaySecs);

    public abstract Builder setTotalTimeoutSecs(int totalTimeoutSecs);

    public abstract SnowflakeHistoricalRetriever build();
  }

  @Override
  public String getStagingLocation() {
    return jobStagingLocation();
  }

  @Override
  public HistoricalRetrievalResult getHistoricalFeatures(
      String retrievalId, DatasetSource datasetSource, List<FeatureSetRequest> featureSetRequests) {
    List<FeatureSetQueryInfo> featureSetQueryInfos =
        QueryTemplater.getFeatureSetInfos(featureSetRequests);

    // 1. load entity table
    String entityTableName;
    try {

    } catch (Exception e) {
      return HistoricalRetrievalResult.error(
          retrievalId,
          new RuntimeException(
              String.format("Unable to load entity table to BigQuery: %s", e.toString())));
    }

    // 2. Retrieve the temporal bounds of the entity dataset provided

    // 3. Generate the subqueries

    try {
      // 4. Run the subqueries in parallel then collect the outputs

      String exportTableDestinationUri =
          String.format("%s/%s/*.avro", jobStagingLocation(), retrievalId);

      // 5. Export the table
      // Hardcode the format to Avro for now

    } catch (Exception e) {
      return HistoricalRetrievalResult.error(retrievalId, e);
    }

    List<String> fileUris = parseOutputFileURIs(retrievalId);

    return HistoricalRetrievalResult.success(
        retrievalId, fileUris, ServingAPIProto.DataFormat.DATA_FORMAT_AVRO);
  }

  private List<String> generateQueries(
      String entityTableName, List<FeatureSetQueryInfo> featureSetQueryInfos) {
    List<String> featureSetQueries = new ArrayList<>();
    try {
      for (FeatureSetQueryInfo featureSetInfo : featureSetQueryInfos) {}

    } catch (Exception e) {
      throw Status.INTERNAL
          .withDescription("Unable to generate query for batch retrieval")
          .withCause(e)
          .asRuntimeException();
    }
    return featureSetQueries;
  }

  void runBatchQuery(
      String entityTableName,
      List<String> entityTableColumnNames,
      List<FeatureSetQueryInfo> featureSetQueryInfos,
      List<String> featureSetQueries)
      throws InterruptedException, IOException {
    ExecutorService executorService = Executors.newFixedThreadPool(featureSetQueries.size());
    ExecutorCompletionService<FeatureSetQueryInfo> executorCompletionService =
        new ExecutorCompletionService<>(executorService);

    // For each of the feature sets requested, start an async job joining the features in that
    // feature set to the provided entity table
    for (int i = 0; i < featureSetQueries.size(); i++) {
      executorCompletionService.submit(null);
    }

    List<FeatureSetQueryInfo> completedFeatureSetQueryInfos = new ArrayList<>();

    for (int i = 0; i < featureSetQueries.size(); i++) {
      try {
        // Try to retrieve the outputs of all the jobs. The timeout here is a formality;
        // a stricter timeout is implemented in the actual SubqueryCallable.
        FeatureSetQueryInfo featureSetInfo =
            executorCompletionService.take().get(SUBQUERY_TIMEOUT_SECS, TimeUnit.SECONDS);
        completedFeatureSetQueryInfos.add(featureSetInfo);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        executorService.shutdownNow();
        throw Status.INTERNAL
            .withDescription("Error running batch query")
            .withCause(e)
            .asRuntimeException();
      }
    }

    // Generate and run a join query to collect the outputs of all the
    // subqueries into a single table.
    String joinQuery =
        QueryTemplater.createJoinQuery(
            completedFeatureSetQueryInfos, entityTableColumnNames, entityTableName);
  }

  private List<String> parseOutputFileURIs(String feastJobId) {
    String scheme = jobStagingLocation().substring(0, jobStagingLocation().indexOf("://"));
    String stagingLocationNoScheme =
        jobStagingLocation().substring(jobStagingLocation().indexOf("://") + 3);
    String bucket = stagingLocationNoScheme.split("/")[0];
    List<String> prefixParts = new ArrayList<>();
    prefixParts.add(
        stagingLocationNoScheme.contains("/") && !stagingLocationNoScheme.endsWith("/")
            ? stagingLocationNoScheme.substring(stagingLocationNoScheme.indexOf("/") + 1)
            : "");
    prefixParts.add(feastJobId);
    String prefix = String.join("/", prefixParts) + "/";

    List<String> fileUris = new ArrayList<>();

    return fileUris;
  }

  public String createTempTableName() {
    return "_" + UUID.randomUUID().toString().replace("-", "");
  }
}
