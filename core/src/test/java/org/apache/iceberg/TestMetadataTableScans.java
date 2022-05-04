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

package org.apache.iceberg;

import java.io.File;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestMetadataTableScans extends TableTestBase {

  @Parameterized.Parameters(name = "formatVersion = {0}")
  public static Object[] parameters() {
    return new Object[] { 1, 2 };
  }

  public TestMetadataTableScans(int formatVersion) {
    super(formatVersion);
  }

  private void preparePartitionedTable() {
    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_C)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_D)
        .commit();
    table.newFastAppend()
        .appendFile(FILE_B)
        .commit();

    if (formatVersion == 2) {
      table.newRowDelta()
          .addDeletes(FILE_A_DELETES)
          .commit();
      table.newRowDelta()
          .addDeletes(FILE_B_DELETES)
          .commit();
      table.newRowDelta()
          .addDeletes(FILE_C2_DELETES)
          .commit();
      table.newRowDelta()
          .addDeletes(FILE_D2_DELETES)
          .commit();
    }
  }

  @Test
  public void testManifestsTableWithDroppedPartition() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table manifestsTable = new ManifestsTable(table.ops(), table);
    TableScan scan = manifestsTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assert.assertEquals("Should have one task", 1, Iterables.size(tasks));
    }
  }

  @Test
  public void testManifestsTableAlwaysIgnoresResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table manifestsTable = new ManifestsTable(table.ops(), table);

    TableScan scan = manifestsTable.newScan()
        .filter(Expressions.lessThan("length", 10000L));

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assert.assertTrue("Tasks should not be empty", Iterables.size(tasks) > 0);
      for (FileScanTask task : tasks) {
        Assert.assertEquals("Residuals must be ignored", Expressions.alwaysTrue(), task.residual());
      }
    }
  }

  @Test
  public void testDataFilesTableWithDroppedPartition() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table dataFilesTable = new DataFilesTable(table.ops(), table);
    TableScan scan = dataFilesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assert.assertEquals("Should have one task", 1, Iterables.size(tasks));
    }
  }

  @Test
  public void testDataFilesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table dataFilesTable = new DataFilesTable(table.ops(), table);

    TableScan scan1 = dataFilesTable.newScan()
        .filter(Expressions.equal("record_count", 1));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 = dataFilesTable.newScan()
        .filter(Expressions.equal("record_count", 1))
        .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testManifestEntriesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table manifestEntriesTable = new ManifestEntriesTable(table.ops(), table);

    TableScan scan1 = manifestEntriesTable.newScan()
        .filter(Expressions.equal("snapshot_id", 1L));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 = manifestEntriesTable.newScan()
        .filter(Expressions.equal("snapshot_id", 1L))
        .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testManifestEntriesTableWithDroppedPartition() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table manifestEntriesTable = new ManifestEntriesTable(table.ops(), table);
    TableScan scan = manifestEntriesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assert.assertEquals("Should have one task", 1, Iterables.size(tasks));
    }
  }

  @Test
  public void testAllDataFilesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table allDataFilesTable = new AllDataFilesTable(table.ops(), table);

    TableScan scan1 = allDataFilesTable.newScan()
        .filter(Expressions.equal("record_count", 1));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 = allDataFilesTable.newScan()
        .filter(Expressions.equal("record_count", 1))
        .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testAllDataFilesTableWithDroppedPartition() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table allDataFilesTable = new AllDataFilesTable(table.ops(), table);
    TableScan scan = allDataFilesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assert.assertEquals("Should have one task", 1, Iterables.size(tasks));
    }
  }

  @Test
  public void testAllEntriesTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table allEntriesTable = new AllEntriesTable(table.ops(), table);

    TableScan scan1 = allEntriesTable.newScan()
        .filter(Expressions.equal("snapshot_id", 1L));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 = allEntriesTable.newScan()
        .filter(Expressions.equal("snapshot_id", 1L))
        .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testAllEntriesTableWithDroppedPartition() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table allEntriesTable = new AllEntriesTable(table.ops(), table);
    TableScan scan = allEntriesTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assert.assertEquals("Should have one task", 1, Iterables.size(tasks));
    }
  }

  @Test
  public void testAllManifestsTableWithDroppedPartition() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table allManifestsTable = new AllManifestsTable(table.ops(), table);

    TableScan scan = allManifestsTable.newScan();

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Assert.assertEquals("Should have one task", 1, Iterables.size(tasks));
    }
  }

  @Test
  public void testAllManifestsTableHonorsIgnoreResiduals() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table allManifestsTable = new AllManifestsTable(table.ops(), table);

    TableScan scan1 = allManifestsTable.newScan()
        .filter(Expressions.lessThan("length", 10000L));
    validateTaskScanResiduals(scan1, false);

    TableScan scan2 = allManifestsTable.newScan()
        .filter(Expressions.lessThan("length", 10000L))
        .ignoreResiduals();
    validateTaskScanResiduals(scan2, true);
  }

  @Test
  public void testPartitionsTableScanNoFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table.ops(), table);
    Types.StructType expected = new Schema(
        required(1, "partition", Types.StructType.of(
            optional(1000, "data_bucket", Types.IntegerType.get())))).asStruct();

    TableScan scanNoFilter = partitionsTable.newScan().select("partition.data_bucket");
    Assert.assertEquals(expected, scanNoFilter.schema().asStruct());
    CloseableIterable<FileScanTask> tasksNoFilter = PartitionsTable.planFiles((StaticTableScan) scanNoFilter);
    Assert.assertEquals(4, Iterators.size(tasksNoFilter.iterator()));
    validateIncludesPartitionScan(tasksNoFilter, 0);
    validateIncludesPartitionScan(tasksNoFilter, 1);
    validateIncludesPartitionScan(tasksNoFilter, 2);
    validateIncludesPartitionScan(tasksNoFilter, 3);
  }

  @Test
  public void testPartitionsTableScanNoStats() {
    table.newFastAppend()
            .appendFile(FILE_WITH_STATS)
            .commit();

    Table partitionsTable = new PartitionsTable(table.ops(), table);
    CloseableIterable<FileScanTask> tasksAndEq = PartitionsTable.planFiles((StaticTableScan) partitionsTable.newScan());
    for (FileScanTask fileTask : tasksAndEq) {
      Assert.assertNull(fileTask.file().columnSizes());
      Assert.assertNull(fileTask.file().valueCounts());
      Assert.assertNull(fileTask.file().nullValueCounts());
      Assert.assertNull(fileTask.file().lowerBounds());
      Assert.assertNull(fileTask.file().upperBounds());
    }
  }

  @Test
  public void testPartitionsTableScanAndFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression andEquals = Expressions.and(
        Expressions.equal("partition.data_bucket", 0),
        Expressions.greaterThan("record_count", 0),
        Expressions.greaterThan("size_in_bytes", 0));
    TableScan scanAndEq = partitionsTable.newScan().filter(andEquals);
    CloseableIterable<FileScanTask> tasksAndEq = PartitionsTable.planFiles((StaticTableScan) scanAndEq);
    Assert.assertEquals(1, Iterators.size(tasksAndEq.iterator()));
    validateIncludesPartitionScan(tasksAndEq, 0);
  }

  @Test
  public void testPartitionsTableScanLtFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression ltAnd = Expressions.and(
        Expressions.lessThan("partition.data_bucket", 2),
        Expressions.greaterThan("record_count", 0),
        Expressions.greaterThan("size_in_bytes", 0));
    TableScan scanLtAnd = partitionsTable.newScan().filter(ltAnd);
    CloseableIterable<FileScanTask> tasksLtAnd = PartitionsTable.planFiles((StaticTableScan) scanLtAnd);
    Assert.assertEquals(2, Iterators.size(tasksLtAnd.iterator()));
    validateIncludesPartitionScan(tasksLtAnd, 0);
    validateIncludesPartitionScan(tasksLtAnd, 1);
  }

  @Test
  public void testPartitionsTableScanOrFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression or = Expressions.or(
        Expressions.equal("partition.data_bucket", 2),
        Expressions.greaterThan("record_count", 0));
    TableScan scanOr = partitionsTable.newScan().filter(or);
    CloseableIterable<FileScanTask> tasksOr = PartitionsTable.planFiles((StaticTableScan) scanOr);
    Assert.assertEquals(4, Iterators.size(tasksOr.iterator()));
    validateIncludesPartitionScan(tasksOr, 0);
    validateIncludesPartitionScan(tasksOr, 1);
    validateIncludesPartitionScan(tasksOr, 2);
    validateIncludesPartitionScan(tasksOr, 3);

    or = Expressions.or(
        Expressions.equal("partition.data_bucket", 2),
        Expressions.greaterThan("size_in_bytes", 0));
    scanOr = partitionsTable.newScan().filter(or);
    tasksOr = PartitionsTable.planFiles((StaticTableScan) scanOr);
    Assert.assertEquals(4, Iterators.size(tasksOr.iterator()));
    validateIncludesPartitionScan(tasksOr, 0);
    validateIncludesPartitionScan(tasksOr, 1);
    validateIncludesPartitionScan(tasksOr, 2);
    validateIncludesPartitionScan(tasksOr, 3);
  }


  @Test
  public void testPartitionsScanNotFilter() {
    preparePartitionedTable();
    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression not = Expressions.not(Expressions.lessThan("partition.data_bucket", 2));
    TableScan scanNot = partitionsTable.newScan().filter(not);
    CloseableIterable<FileScanTask> tasksNot = PartitionsTable.planFiles((StaticTableScan) scanNot);
    Assert.assertEquals(2, Iterators.size(tasksNot.iterator()));
    validateIncludesPartitionScan(tasksNot, 2);
    validateIncludesPartitionScan(tasksNot, 3);
  }

  @Test
  public void testPartitionsTableScanInFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression set = Expressions.in("partition.data_bucket", 2, 3);
    TableScan scanSet = partitionsTable.newScan().filter(set);
    CloseableIterable<FileScanTask> tasksSet = PartitionsTable.planFiles((StaticTableScan) scanSet);
    Assert.assertEquals(2, Iterators.size(tasksSet.iterator()));
    validateIncludesPartitionScan(tasksSet, 2);
    validateIncludesPartitionScan(tasksSet, 3);
  }

  @Test
  public void testPartitionsTableScanNotNullFilter() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression unary = Expressions.notNull("partition.data_bucket");
    TableScan scanUnary = partitionsTable.newScan().filter(unary);
    CloseableIterable<FileScanTask> tasksUnary = PartitionsTable.planFiles((StaticTableScan) scanUnary);
    Assert.assertEquals(4, Iterators.size(tasksUnary.iterator()));
    validateIncludesPartitionScan(tasksUnary, 0);
    validateIncludesPartitionScan(tasksUnary, 1);
    validateIncludesPartitionScan(tasksUnary, 2);
    validateIncludesPartitionScan(tasksUnary, 3);
  }

  @Test
  public void testFilesTableScanWithDroppedPartition() throws IOException {
    preparePartitionedTable();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().removeField(Expressions.bucket("data", 16)).commit();
    table.refresh();

    table.updateSpec().addField(Expressions.truncate("data", 2)).commit();

    Table dataFilesTable = new DataFilesTable(table.ops(), table);
    TableScan scan = dataFilesTable.newScan();

    Schema schema = dataFilesTable.schema();
    Types.StructType actualType = schema.findField(DataFile.PARTITION_ID).type().asStructType();
    Types.StructType expectedType = Types.StructType.of(
        Types.NestedField.optional(1000, "data_bucket", Types.IntegerType.get()),
        Types.NestedField.optional(1001, "data_bucket_16", Types.IntegerType.get()),
        Types.NestedField.optional(1002, "data_trunc_2", Types.StringType.get())
    );
    Assert.assertEquals("Partition type must match", expectedType, actualType);
    Accessor<StructLike> accessor = schema.accessorForField(1000);

    try (CloseableIterable<FileScanTask> tasks = scan.planFiles()) {
      Set<Integer> results = StreamSupport.stream(tasks.spliterator(), false)
          .flatMap(fileScanTask -> Streams.stream(fileScanTask.asDataTask().rows()))
          .map(accessor::get).map(i -> (Integer) i).collect(Collectors.toSet());
      Assert.assertEquals("Partition value must match", Sets.newHashSet(0, 1, 2, 3), results);
    }
  }

  @Test
  public void testDeleteFilesTableSelection() throws IOException {
    Assume.assumeTrue("Only V2 Tables Support Deletes", formatVersion >= 2);

    table.newFastAppend()
        .appendFile(FILE_A)
        .commit();

    table.newRowDelta()
        .addDeletes(FILE_A_DELETES)
        .addDeletes(FILE_A2_DELETES)
        .commit();

    Table deleteFilesTable = new DeleteFilesTable(table.ops(), table);

    TableScan scan = deleteFilesTable.newScan()
        .filter(Expressions.equal("record_count", 1))
        .select("content", "record_count");
    validateTaskScanResiduals(scan, false);
    Types.StructType expected = new Schema(
        optional(134, "content", Types.IntegerType.get(),
            "Contents of the file: 0=data, 1=position deletes, 2=equality deletes"),
        required(103, "record_count", Types.LongType.get(), "Number of records in the file")
    ).asStruct();
    Assert.assertEquals(expected, scan.schema().asStruct());
  }

  @Test
  public void testPartitionColumnNamedPartition() throws Exception {
    TestTables.clearTables();
    this.tableDir = temp.newFolder();
    tableDir.delete();

    Schema schema = new Schema(
        required(1, "id", Types.IntegerType.get()),
        required(2, "partition", Types.IntegerType.get())
    );
    this.metadataDir = new File(tableDir, "metadata");
    PartitionSpec spec = PartitionSpec.builderFor(schema)
        .identity("partition")
        .build();

    DataFile par0 = DataFiles.builder(spec)
        .withPath("/path/to/data-0.parquet")
        .withFileSizeInBytes(10)
        .withPartition(TestHelpers.Row.of(0))
        .withRecordCount(1)
        .build();
    DataFile par1 = DataFiles.builder(spec)
        .withPath("/path/to/data-0.parquet")
        .withFileSizeInBytes(10)
        .withPartition(TestHelpers.Row.of(1))
        .withRecordCount(1)
        .build();
    DataFile par2 = DataFiles.builder(spec)
        .withPath("/path/to/data-0.parquet")
        .withFileSizeInBytes(10)
        .withPartition(TestHelpers.Row.of(2))
        .withRecordCount(1)
        .build();

    this.table = create(schema, spec);
    table.newFastAppend()
        .appendFile(par0)
        .commit();
    table.newFastAppend()
        .appendFile(par1)
        .commit();
    table.newFastAppend()
        .appendFile(par2)
        .commit();

    Table partitionsTable = new PartitionsTable(table.ops(), table);

    Expression andEquals = Expressions.and(
        Expressions.equal("partition.partition", 0),
        Expressions.greaterThan("record_count", 0));
    TableScan scanAndEq = partitionsTable.newScan().filter(andEquals);
    CloseableIterable<FileScanTask> tasksAndEq = PartitionsTable.planFiles((StaticTableScan) scanAndEq);
    Assert.assertEquals(1, Iterators.size(tasksAndEq.iterator()));
    validateIncludesPartitionScan(tasksAndEq, 0);
  }


  @Test
  public void testAllDataFilesTableScanWithPlanExecutor() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table allDataFilesTable = new AllDataFilesTable(table.ops(), table);
    AtomicInteger planThreadsIndex = new AtomicInteger(0);
    TableScan scan = allDataFilesTable.newScan()
        .planWith(Executors.newFixedThreadPool(1, runnable -> {
          Thread thread = new Thread(runnable);
          thread.setName("plan-" + planThreadsIndex.getAndIncrement());
          thread.setDaemon(true); // daemon threads will be terminated abruptly when the JVM exits
          return thread;
        }));
    Assert.assertEquals(1, Iterables.size(scan.planFiles()));
    Assert.assertTrue("Thread should be created in provided pool", planThreadsIndex.get() > 0);
  }

  @Test
  public void testAllEntriesTableScanWithPlanExecutor() throws IOException {
    table.newFastAppend()
        .appendFile(FILE_A)
        .appendFile(FILE_B)
        .commit();

    Table allEntriesTable = new AllEntriesTable(table.ops(), table);
    AtomicInteger planThreadsIndex = new AtomicInteger(0);
    TableScan scan = allEntriesTable.newScan()
        .planWith(Executors.newFixedThreadPool(1, runnable -> {
          Thread thread = new Thread(runnable);
          thread.setName("plan-" + planThreadsIndex.getAndIncrement());
          thread.setDaemon(true); // daemon threads will be terminated abruptly when the JVM exits
          return thread;
        }));
    Assert.assertEquals(1, Iterables.size(scan.planFiles()));
    Assert.assertTrue("Thread should be created in provided pool", planThreadsIndex.get() > 0);
  }

  @Test
  public void testPartitionsTableScanWithPlanExecutor() {
    preparePartitionedTable();

    Table partitionsTable = new PartitionsTable(table.ops(), table);
    AtomicInteger planThreadsIndex = new AtomicInteger(0);
    TableScan scan = partitionsTable.newScan()
        .planWith(Executors.newFixedThreadPool(1, runnable -> {
          Thread thread = new Thread(runnable);
          thread.setName("plan-" + planThreadsIndex.getAndIncrement());
          thread.setDaemon(true); // daemon threads will be terminated abruptly when the JVM exits
          return thread;
        }));
    CloseableIterable<FileScanTask> tasks = PartitionsTable.planFiles((StaticTableScan) scan);
    Assert.assertEquals(4, Iterables.size(tasks));
    Assert.assertTrue("Thread should be created in provided pool", planThreadsIndex.get() > 0);
  }

  private void validateTaskScanResiduals(TableScan scan, boolean ignoreResiduals) throws IOException {
    try (CloseableIterable<CombinedScanTask> tasks = scan.planTasks()) {
      Assert.assertTrue("Tasks should not be empty", Iterables.size(tasks) > 0);
      for (CombinedScanTask combinedScanTask : tasks) {
        for (FileScanTask fileScanTask : combinedScanTask.files()) {
          if (ignoreResiduals) {
            Assert.assertEquals("Residuals must be ignored", Expressions.alwaysTrue(), fileScanTask.residual());
          } else {
            Assert.assertNotEquals("Residuals must be preserved", Expressions.alwaysTrue(), fileScanTask.residual());
          }
        }
      }
    }
  }

  private void validateIncludesPartitionScan(CloseableIterable<FileScanTask> tasks, int partValue) {
    Assert.assertTrue("File scan tasks do not include correct file",
        StreamSupport.stream(tasks.spliterator(), false).anyMatch(
            a -> a.file().partition().get(0, Object.class).equals(partValue)));
  }

  private boolean manifestHasPartition(ManifestFile mf, int partValue) {
    int lower = Conversions.fromByteBuffer(Types.IntegerType.get(), mf.partitions().get(0).lowerBound());
    int upper = Conversions.fromByteBuffer(Types.IntegerType.get(), mf.partitions().get(0).upperBound());
    return (lower <= partValue) && (upper >= partValue);
  }
}
