/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.io;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public abstract class TestRollingFileWriters<T> extends WriterTestBase<T> {

  @Parameters(name = "formatVersion = {0}, fileFormat = {1}, Partitioned = {2}")
  protected static List<Object> parameters() {
    return Arrays.asList(
        new Object[] {2, FileFormat.AVRO, false},
        new Object[] {2, FileFormat.AVRO, true},
        new Object[] {2, FileFormat.PARQUET, false},
        new Object[] {2, FileFormat.PARQUET, true},
        new Object[] {2, FileFormat.ORC, false},
        new Object[] {2, FileFormat.ORC, true});
  }

  private static final int FILE_SIZE_CHECK_ROWS_DIVISOR = 1000;
  private static final long DEFAULT_FILE_SIZE = 128L * 1024 * 1024;
  private static final long SMALL_FILE_SIZE = 2L;
  private static final String PARTITION_VALUE = "aaa";

  @Parameter(index = 1)
  private FileFormat fileFormat;

  @Parameter(index = 2)
  private boolean partitioned;

  private StructLike partition = null;
  private OutputFileFactory fileFactory = null;

  protected FileFormat format() {
    return fileFormat;
  }

  @Override
  @BeforeEach
  public void setupTable() throws Exception {
    this.metadataDir = new File(tableDir, "metadata");

    if (partitioned) {
      this.table = create(SCHEMA, SPEC);
      this.partition = partitionKey(table.spec(), PARTITION_VALUE);
    } else {
      this.table = create(SCHEMA, PartitionSpec.unpartitioned());
      this.partition = null;
    }

    this.fileFactory = OutputFileFactory.builderFor(table, 1, 1).format(fileFormat).build();
  }

  @TestTemplate
  public void testRollingDataWriterNoRecords() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    RollingDataWriter<T> writer =
        new RollingDataWriter<>(
            writerFactory, fileFactory, table.io(), DEFAULT_FILE_SIZE, table.spec(), partition);

    writer.close();
    assertThat(writer.result().dataFiles()).isEmpty();

    writer.close();
    assertThat(writer.result().dataFiles()).isEmpty();
  }

  @TestTemplate
  public void testRollingDataWriterSplitData() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    RollingDataWriter<T> writer =
        new RollingDataWriter<>(
            writerFactory, fileFactory, table.io(), SMALL_FILE_SIZE, table.spec(), partition);

    List<T> rows = Lists.newArrayListWithExpectedSize(4 * FILE_SIZE_CHECK_ROWS_DIVISOR);
    for (int index = 0; index < 4 * FILE_SIZE_CHECK_ROWS_DIVISOR; index++) {
      rows.add(toRow(index, PARTITION_VALUE));
    }

    try (RollingDataWriter<T> closableWriter = writer) {
      closableWriter.write(rows);
    }

    // call close again to ensure it is idempotent
    writer.close();

    assertThat(writer.result().dataFiles()).hasSize(4);
  }

  @TestTemplate
  public void testRollingEqualityDeleteWriterNoRecords() throws IOException {
    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    Schema equalityDeleteRowSchema = table.schema().select("id");
    FileWriterFactory<T> writerFactory =
        newWriterFactory(table.schema(), equalityFieldIds, equalityDeleteRowSchema);
    RollingEqualityDeleteWriter<T> writer =
        new RollingEqualityDeleteWriter<>(
            writerFactory, fileFactory, table.io(), DEFAULT_FILE_SIZE, table.spec(), partition);

    writer.close();
    assertThat(writer.result().deleteFiles()).isEmpty();
    assertThat(writer.result().referencedDataFiles()).isEmpty();
    assertThat(writer.result().referencesDataFiles()).isFalse();

    writer.close();
    assertThat(writer.result().deleteFiles()).isEmpty();
    assertThat(writer.result().referencedDataFiles()).isEmpty();
    assertThat(writer.result().referencesDataFiles()).isFalse();
  }

  @TestTemplate
  public void testRollingEqualityDeleteWriterSplitDeletes() throws IOException {
    List<Integer> equalityFieldIds = ImmutableList.of(table.schema().findField("id").fieldId());
    Schema equalityDeleteRowSchema = table.schema().select("id");
    FileWriterFactory<T> writerFactory =
        newWriterFactory(table.schema(), equalityFieldIds, equalityDeleteRowSchema);
    RollingEqualityDeleteWriter<T> writer =
        new RollingEqualityDeleteWriter<>(
            writerFactory, fileFactory, table.io(), SMALL_FILE_SIZE, table.spec(), partition);

    List<T> deletes = Lists.newArrayListWithExpectedSize(4 * FILE_SIZE_CHECK_ROWS_DIVISOR);
    for (int index = 0; index < 4 * FILE_SIZE_CHECK_ROWS_DIVISOR; index++) {
      deletes.add(toRow(index, PARTITION_VALUE));
    }

    try (RollingEqualityDeleteWriter<T> closeableWriter = writer) {
      closeableWriter.write(deletes);
    }

    // call close again to ensure it is idempotent
    writer.close();

    assertThat(writer.result().deleteFiles()).hasSize(4);
    assertThat(writer.result().referencedDataFiles()).isEmpty();
    assertThat(writer.result().referencesDataFiles()).isFalse();
  }

  @TestTemplate
  public void testRollingPositionDeleteWriterNoRecords() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    RollingPositionDeleteWriter<T> writer =
        new RollingPositionDeleteWriter<>(
            writerFactory, fileFactory, table.io(), DEFAULT_FILE_SIZE, table.spec(), partition);

    writer.close();
    assertThat(writer.result().deleteFiles()).isEmpty();
    assertThat(writer.result().referencedDataFiles()).isEmpty();
    assertThat(writer.result().referencesDataFiles()).isFalse();

    writer.close();
    assertThat(writer.result().deleteFiles()).isEmpty();
    assertThat(writer.result().referencedDataFiles()).isEmpty();
    assertThat(writer.result().referencesDataFiles()).isFalse();
  }

  @TestTemplate
  public void testRollingPositionDeleteWriterSplitDeletes() throws IOException {
    FileWriterFactory<T> writerFactory = newWriterFactory(table.schema());
    RollingPositionDeleteWriter<T> writer =
        new RollingPositionDeleteWriter<>(
            writerFactory, fileFactory, table.io(), SMALL_FILE_SIZE, table.spec(), partition);

    List<PositionDelete<T>> deletes =
        Lists.newArrayListWithExpectedSize(4 * FILE_SIZE_CHECK_ROWS_DIVISOR);
    for (int index = 0; index < 4 * FILE_SIZE_CHECK_ROWS_DIVISOR; index++) {
      deletes.add(positionDelete("path/to/data/file-1.parquet", index, null));
    }

    try (RollingPositionDeleteWriter<T> closeableWriter = writer) {
      closeableWriter.write(deletes);
    }

    // call close again to ensure it is idempotent
    writer.close();

    assertThat(writer.result().deleteFiles()).hasSize(4);
    assertThat(writer.result().referencedDataFiles()).hasSize(1);
    assertThat(writer.result().referencesDataFiles()).isTrue();
  }
}
