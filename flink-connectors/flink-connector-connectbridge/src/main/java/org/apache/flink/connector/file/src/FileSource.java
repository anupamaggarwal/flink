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

package org.apache.flink.connector.file.src;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.assigners.LocalityAwareSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.BlockSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.impl.FileRecordFormatAdapter;
import org.apache.flink.connector.file.src.impl.StreamFormatAdapter;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.reader.FileRecordFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import javax.annotation.Nullable;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;


@PublicEvolving
public final class FileSource<T> extends AbstractFileSource<T, FileSourceSplit> {

    private static final long serialVersionUID = 1L;

    /** The default split assigner, a lazy locality-aware assigner. */
    public static final FileSplitAssigner.Provider DEFAULT_SPLIT_ASSIGNER =
            LocalityAwareSplitAssigner::new;

    /**
     * The default file enumerator used for splittable formats. The enumerator recursively
     * enumerates files, split files that consist of multiple distributed storage blocks into
     * multiple splits, and filters hidden files (files starting with '.' or '_'). Files with
     * suffixes of common compression formats (for example '.gzip', '.bz2', '.xy', '.zip', ...) will
     * not be split.
     */
    public static final FileEnumerator.Provider DEFAULT_SPLITTABLE_FILE_ENUMERATOR =
            BlockSplittingRecursiveEnumerator::new;

    /**
     * The default file enumerator used for non-splittable formats. The enumerator recursively
     * enumerates files, creates one split for the file, and filters hidden files (files starting
     * with '.' or '_').
     */
    public static final FileEnumerator.Provider DEFAULT_NON_SPLITTABLE_FILE_ENUMERATOR =
            NonSplittingRecursiveEnumerator::new;

    // ------------------------------------------------------------------------

    private FileSource(
            final Path[] inputPaths,
            final FileEnumerator.Provider fileEnumerator,
            final FileSplitAssigner.Provider splitAssigner,
            final BulkFormat<T, FileSourceSplit> readerFormat,
            @Nullable final ContinuousEnumerationSettings continuousEnumerationSettings) {

        super(
                inputPaths,
                fileEnumerator,
                splitAssigner,
                readerFormat,
                continuousEnumerationSettings);
    }

    @Override
    public SimpleVersionedSerializer<FileSourceSplit> getSplitSerializer() {
        return FileSourceSplitSerializer.INSTANCE;
    }

    // ------------------------------------------------------------------------
    //  Entry-point Factory Methods
    // ------------------------------------------------------------------------

    /**
     * Builds a new {@code FileSource} using a {@link StreamFormat} to read record-by-record from a
     * file stream.
     *
     * <p>When possible, stream-based formats are generally easier (preferable) to file-based
     * formats, because they support better default behavior around I/O batching or progress
     * tracking (checkpoints).
     *
     * <p>Stream formats also automatically de-compress files based on the file extension. This
     * supports files ending in ".deflate" (Deflate), ".xz" (XZ), ".bz2" (BZip2), ".gz", ".gzip"
     * (GZip).
     */
    public static <T> FileSourceBuilder<T> forRecordStreamFormat(
            final StreamFormat<T> streamFormat, final Path... paths) {
        return forBulkFileFormat(new StreamFormatAdapter<>(streamFormat), paths);
    }

    /**
     * Builds a new {@code FileSource} using a {@link BulkFormat} to read batches of records from
     * files.
     *
     * <p>Examples for bulk readers are compressed and vectorized formats such as ORC or Parquet.
     */
    public static <T> FileSourceBuilder<T> forBulkFileFormat(
            final BulkFormat<T, FileSourceSplit> bulkFormat, final Path... paths) {
        checkNotNull(bulkFormat, "reader");
        checkNotNull(paths, "paths");
        checkArgument(paths.length > 0, "paths must not be empty");

        return new FileSourceBuilder<>(paths, bulkFormat);
    }

    /**
     * Builds a new {@code FileSource} using a {@link FileRecordFormat} to read record-by-record
     * from a a file path.
     *
     * <p>A {@code FileRecordFormat} is more general than the {@link StreamFormat}, but also
     * requires often more careful parametrization.
     *
     * @deprecated Please use {@link #forRecordStreamFormat(StreamFormat, Path...)} instead.
     */
    @Deprecated
    public static <T> FileSourceBuilder<T> forRecordFileFormat(
            final FileRecordFormat<T> recordFormat, final Path... paths) {
        return forBulkFileFormat(new FileRecordFormatAdapter<>(recordFormat), paths);
    }

    // ------------------------------------------------------------------------
    //  Builder
    // ------------------------------------------------------------------------

    /**
     * The builder for the {@code FileSource}, to configure the various behaviors.
     *
     * <p>Start building the source via one of the following methods:
     *
     * <ul>
     *   <li>{@link FileSource#forRecordStreamFormat(StreamFormat, Path...)}
     *   <li>{@link FileSource#forBulkFileFormat(BulkFormat, Path...)}
     * </ul>
     */
    public static final class FileSourceBuilder<T>
            extends AbstractFileSourceBuilder<T, FileSourceSplit, FileSourceBuilder<T>> {

        FileSourceBuilder(Path[] inputPaths, BulkFormat<T, FileSourceSplit> readerFormat) {
            super(
                    inputPaths,
                    readerFormat,
                    readerFormat.isSplittable()
                            ? DEFAULT_SPLITTABLE_FILE_ENUMERATOR
                            : DEFAULT_NON_SPLITTABLE_FILE_ENUMERATOR,
                    DEFAULT_SPLIT_ASSIGNER);
        }

        @Override
        public FileSource<T> build() {
            return new FileSource<>(
                    inputPaths,
                    fileEnumerator,
                    splitAssigner,
                    readerFormat,
                    continuousSourceSettings);
        }
    }
}
