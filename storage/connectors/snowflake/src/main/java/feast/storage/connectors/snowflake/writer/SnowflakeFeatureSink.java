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
package feast.storage.connectors.snowflake.writer;

import com.google.auto.value.AutoValue;
import feast.core.FeatureSetProto;
import feast.storage.api.writer.FeatureSink;
import feast.storage.api.writer.WriteResult;
import feast.types.FeatureRowProto;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;

@AutoValue
public abstract class SnowflakeFeatureSink implements FeatureSink {
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(SnowflakeFeatureSink.class);

  // Column description for reserved fields
  public static final String SF_EVENT_TIMESTAMP_FIELD_DESCRIPTION = "Event time for the FeatureRow";
  public static final String SF_CREATED_TIMESTAMP_FIELD_DESCRIPTION =
      "Processing time of the FeatureRow ingestion in Feast\"";
  public static final String SF_INGESTION_ID_FIELD_DESCRIPTION =
      "Unique id identifying groups of rows that have been ingested together";
  public static final String SF_JOB_ID_FIELD_DESCRIPTION = "Feast import job ID for the FeatureRow";

  public abstract String getProjectId();

  public abstract String getDatasetId();

  public static Builder builder() {
    return new AutoValue_SnowflakeFeatureSink.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {

    public abstract Builder setProjectId(String projectId);

    public abstract Builder setDatasetId(String datasetId);

    public abstract SnowflakeFeatureSink build();
  }

  /** @param featureSet Feature set to be written */
  @Override
  public void prepareWrite(FeatureSetProto.FeatureSet featureSet) {
    FeatureSetProto.FeatureSetSpec featureSetSpec = featureSet.getSpec();

    String tableName =
        String.format(
                "%s_%s_v%d",
                featureSetSpec.getProject(), featureSetSpec.getName(), featureSetSpec.getVersion())
            .replaceAll("-", "_");
  }

  @Override
  public PTransform<PCollection<FeatureRowProto.FeatureRow>, WriteResult> writer() {
    return new SnowflakeWrite();
  }
}
