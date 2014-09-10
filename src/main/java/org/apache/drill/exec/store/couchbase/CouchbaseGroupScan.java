/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.couchbase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;

import parquet.org.codehaus.jackson.annotate.JsonCreator;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;

@JsonTypeName("couchbase-scan")
public class CouchbaseGroupScan extends AbstractGroupScan implements DrillCouchbaseConstants {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CouchbaseGroupScan.class);

  private CouchbaseStoragePluginConfig storagePluginConfig;
  private CouchbaseStoragePlugin storagePlugin;
  private String bucket;

  @JsonCreator
  public CouchbaseGroupScan(@JsonProperty("bucket") String bucket,
      @JsonProperty("storage") CouchbaseStoragePluginConfig storagePluginConfig,
      @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
    this((CouchbaseStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), bucket);
  }

  public CouchbaseGroupScan(CouchbaseStoragePlugin storagePlugin, String bucket) {
    this.storagePlugin = storagePlugin;
    this.storagePluginConfig = storagePlugin.getConfig();
    this.bucket = bucket;
  }

  /**
   * Private constructor, used for cloning.
   *
   * @param that
   *          The HBaseGroupScan to clone
   */
  private CouchbaseGroupScan(CouchbaseGroupScan that) {
    this.storagePlugin = that.storagePlugin;
    this.storagePluginConfig = that.storagePluginConfig;
    this.bucket = that.bucket;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    CouchbaseGroupScan newScan = new CouchbaseGroupScan(this);
    return newScan;
  }


  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    return Collections.emptyList();
  }

  /**
   *
   * @param incomingEndpoints
   */
  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    assert incomingEndpoints.size() == 1;
  }

  @Override
  public CouchbaseSubScan getSpecificScan(int minorFragmentId) {
    return new CouchbaseSubScan(this.storagePlugin, this.storagePluginConfig, this.bucket);
  }

  @Override
  public int getMaxParallelizationWidth() {
    return 1;
  }

  @Override
  public ScanStats getScanStats() {
    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, 1000, 1, 1);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new CouchbaseGroupScan(this);
  }

  @JsonIgnore
  public CouchbaseStoragePlugin getStoragePlugin() {
    return storagePlugin;
  }


  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "CouchbaseGroupScan [bucket= " + this.bucket + "]";
  }

  @JsonProperty("storage")
  public CouchbaseStoragePluginConfig getStorageConfig() {
    return this.storagePluginConfig;
  }


  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return false;
  }


}
