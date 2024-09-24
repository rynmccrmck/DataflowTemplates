/*
 * Copyright (C) 2023 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.bigtable.transforms;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.auto.value.AutoValue;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/** Common transforms for Teleport Bigtable templates. */
public class BigtableConverters {

  /** Converts from the BigQuery Avro format into Bigtable mutation. */
  @AutoValue
  public abstract static class AvroToMutation
      implements SerializableFunction<SchemaAndRecord, Mutation> {

    public abstract String columnFamily();

    public abstract String rowkey();

    /** Builder for AvroToMutation. */
    @AutoValue.Builder
    public abstract static class Builder {

      public abstract Builder setColumnFamily(String value);

      public abstract Builder setRowkey(String rowkey);

      public abstract AvroToMutation build();
    }

    public static Builder newBuilder() {
      return new AutoValue_BigtableConverters_AvroToMutation.Builder();
    }


    public Mutation apply(SchemaAndRecord record) {
      GenericRecord row = record.getRecord();

      Map<String, String> columnTypes = new HashMap<>();
      List<TableFieldSchema> columns = record.getTableSchema().getFields();
      for (TableFieldSchema column : columns) {
        columnTypes.put(column.getName(), column.getType());
      }

      String rowkeyType = columnTypes.get(rowkey());
      Object rowKeyObj = row.get(rowkey());
      byte[] rowkeyBytes;

      if (rowKeyObj == null) {
        throw new IllegalArgumentException("Rowkey is null");
      } else if ("BYTES".equals(rowkeyType)) {
        if (rowKeyObj instanceof ByteBuffer) {
          ByteBuffer buffer = ((ByteBuffer) rowKeyObj).duplicate();
          rowkeyBytes = new byte[buffer.remaining()];
          buffer.get(rowkeyBytes);
        } else if (rowKeyObj instanceof byte[]) {
          // TODO: check if this is needed
          rowkeyBytes = (byte[]) rowKeyObj;
        } else if (rowKeyObj instanceof String) {
          // Decode base64-encoded string
          rowkeyBytes = Base64.getDecoder().decode((String) rowKeyObj);
        } else {
          rowkeyBytes = Bytes.toBytes(rowKeyObj.toString());
        }
      } else {
        // For other types, convert to string and then to bytes
        rowkeyBytes = Bytes.toBytes(rowKeyObj.toString());
      }

      Put put = new Put(rowkeyBytes);

      for (TableFieldSchema column : columns) {
        String columnName = column.getName();
        if (columnName.equals(rowkey())) {
          continue;
        }

        Object columnObj = row.get(columnName);
        String fieldType = column.getType();
        byte[] columnValue = null;

        if (columnObj == null) {
          columnValue = null;
        } else if ("BYTES".equals(fieldType)) {
          if (columnObj instanceof ByteBuffer) {

            ByteBuffer buffer = ((ByteBuffer) columnObj).duplicate();
            columnValue = new byte[buffer.remaining()];
            buffer.get(columnValue);
          } else if (columnObj instanceof byte[]) {
            columnValue = (byte[]) columnObj;
          } else if (columnObj instanceof String) {
            // Decode base64-encoded string
            columnValue = Base64.getDecoder().decode((String) columnObj);
          } else {
            columnValue = Bytes.toBytes(columnObj.toString());
          }
        } else {
          // For other types, convert to string and then to bytes
          columnValue = Bytes.toBytes(columnObj.toString());
        }

        put.addColumn(
                Bytes.toBytes(columnFamily()), Bytes.toBytes(columnName), columnValue);
      }
      return put;
    }

  }
}
