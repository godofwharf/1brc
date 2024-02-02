package dev.morling.onebrc;

/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CalculateAverage_godofwharf {
    private static final String FILE = "./measurements.txt";
    private static final boolean DEBUG = Boolean.parseBoolean(System.getProperty("debug", "false"));
    private static final int NCPU = Runtime.getRuntime().availableProcessors();

    // This array is used for quick conversion of fractional part
    private static final double[] DOUBLES = new double[]{ 0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9 };
    // This array is used for quick conversion from ASCII to digit
    private static final int[] DIGIT_LOOKUP = new int[]{
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, 0, 1,
            2, 3, 4, 5, 6, 7, 8, 9, -1, -1 };
    private static final int MAX_STR_LEN = 108;
    private static final int DEFAULT_HASH_TBL_SIZE = 4096;
    private static final int DEFAULT_PAGE_SIZE = 8_388_608; // 8 MB
    private static final int PAGE_SIZE = Integer.parseInt(System.getProperty("pageSize", STR."\{DEFAULT_PAGE_SIZE}"));

    public static void main(String[] args) throws Exception {
        long startTimeMs = System.currentTimeMillis();
        Map<String, MeasurementAggregator> measurements = compute();
        long time1 = System.nanoTime();
        System.out.println(measurements);
        printDebugMessage("Print took %d ns%n", (System.nanoTime() - time1));
        printDebugMessage("Took %d ms%n", System.currentTimeMillis() - startTimeMs);
        printDebugMessage("Time spent on GC=%d ms%n", ManagementFactory.getGarbageCollectorMXBeans().get(0).getCollectionTime());
        System.exit(0);
    }

    private static Map<String, MeasurementAggregator> compute() throws Exception {
        int nThreads = Integer.parseInt(
                System.getProperty("threads", STR."\{NCPU}"));
        printDebugMessage("Running program with %d threads %n", nThreads);
        Job job = new Job(nThreads - 1);
        job.compute(FILE);
        return job.sort();
    }

    private static void printDebugMessage(final String message,
                                          final Object... args) {
        if (DEBUG) {
            System.err.printf(message, args);
        }
    }

    public static class Job {
        private final int nThreads;
        private final State[] threadLocalStates;
        private final Map<String, MeasurementAggregator> globalMap = new ConcurrentHashMap<>(DEFAULT_HASH_TBL_SIZE);
        private final ExecutorService executorService;

        public Job(final int nThreads) {
            this.threadLocalStates = new State[(nThreads << 4)];
            IntStream.range(0, nThreads << 4)
                    .forEach(i -> threadLocalStates[i] = new State());
            this.nThreads = nThreads;
            this.executorService = Executors.newFixedThreadPool(nThreads);
        }

        private static LineMetadata findNextOccurrenceOfNewLine(final ByteBuffer buffer,
                                                                final int capacity,
                                                                final int offset) {
            int maxLen = capacity - offset;
            byte[] src = new byte[Math.min(MAX_STR_LEN, maxLen)];
            byte[] station = new byte[src.length];
            byte[] temperature = new byte[5];
            buffer.position(offset);
            buffer.get(src);
            int i = 0;
            int j = 0;
            int k = 0;
            boolean isAscii = true;
            boolean afterDelim = false;
            int hashCode = 0;
            for (; i < src.length; i++) {
                byte b = src[i];
                if (b < 0) {
                    isAscii = false;
                }
                if (!afterDelim && b != '\n') {
                    if (b == ';') {
                        afterDelim = true;
                    }
                    else {
                        hashCode = hashCode * 31 + b;
                        station[j++] = b;
                    }
                }
                else if (b != '\n') {
                    temperature[k++] = b;
                }
                else {
                    return new LineMetadata(
                            station, temperature, j, k, offset + i + 1, hashCode, isAscii);
                }
            }
            if (i == 0 & j == 0 && k == 0) {
                hashCode = -1;
            }
            return new LineMetadata(
                    station, temperature, j, k, offset + i, hashCode, isAscii);
        }

        private static void processPage(final byte[] page,
                                        final int pageLen,
                                        final State state) {
            int j = 0;
            while (j < pageLen) {
                long h1 = 1;
                int k = j;
                while (k < pageLen && page[k] != ';') {
                    h1 = h1 * 31 + page[k];
                    k++;
                }
                int temperatureLen = 5;
                if (page[k + 4] == '\n') {
                    temperatureLen = 3;
                }
                else if (page[k + 5] == '\n') {
                    temperatureLen = 4;
                }
                byte[] b = new byte[k - j];
                System.arraycopy(page, j, b, 0, k - j);
                Measurement m = new Measurement(b, NumberUtils.parseDouble2(page, k + 1, temperatureLen), h1);

                int idx = (int) (h1 & (state.map.size - 1));
                // if we find an empty slot, claim the same and return immediately
                if (state.map.tableEntries[idx] == null) {
                    state.map.tableEntries[idx] = new FastHashMap2.TableEntry(
                            m.aggregationKey,
                            new MeasurementAggregator(m.temperature, m.temperature, m.temperature, 1L));
                } else {
                    State.AggregationKey k1 = state.map.tableEntries[idx].key;
                    State.AggregationKey k2 = m.aggregationKey;
                    // match found
                    if (k1.h1 == k2.h1 && k1.station.length == m.aggregationKey.station.length && k1.equals(k2)) {
                        MeasurementAggregator agg = state.map.tableEntries[idx].aggregator;
                        agg.count++;
                        agg.min = m.temperature <= agg.min ? m.temperature : agg.min;
                        agg.max = m.temperature >= agg.max ? m.temperature : agg.max;
                        agg.sum += m.temperature;
                    } else {
                        state.map.update(m, idx);
                    }
                }

                j = k + temperatureLen + 2;
            }
        }

        private static List<Split> breakFileIntoSplits(final RandomAccessFile file,
                                                       final int splitLength,
                                                       final int pageLength,
                                                       final MemorySegment memorySegment,
                                                       final boolean enableChecks)
                throws IOException {
            final List<Split> splits = new ArrayList<>();
            // Try to break the file into multiple splits while ensuring that each split has at least splitLength bytes
            // and ends with '\n' or EOF
            for (long i = 0; i < file.length();) {
                long splitStartOffset = i;
                long splitEndOffset = Math.min(file.length(), splitStartOffset + splitLength); // not inclusive
                if (splitEndOffset == file.length()) { // reached EOF
                    List<Page> pages = breakSplitIntoPages(splitStartOffset, splitEndOffset, pageLength, memorySegment, enableChecks);
                    splits.add(new Split(splitStartOffset, splitEndOffset - splitStartOffset, pages));
                    break;
                }
                // Look past the end offset to find next '\n' or EOF
                long segmentLength = Math.min(MAX_STR_LEN, file.length() - i);
                // Create a new memory segment for reading contents beyond splitEndOffset
                MemorySegment lookahead = memorySegment.asSlice(splitEndOffset, segmentLength);
                ByteBuffer bb = lookahead.asByteBuffer();
                // Find the next offset which has either '\n' or EOF
                LineMetadata lineMetadata = findNextOccurrenceOfNewLine(bb, (int) segmentLength, 0);
                splitEndOffset += lineMetadata.offset;
                if (enableChecks &&
                        memorySegment.asSlice(splitEndOffset - 1, 1).asByteBuffer().get(0) != '\n') {
                    throw new IllegalStateException("Page doesn't end with NL char");
                }
                // Break the split further into multiple pages based on pageLength
                List<Page> pages = breakSplitIntoPages(splitStartOffset, splitEndOffset, pageLength, memorySegment, enableChecks);
                splits.add(new Split(splitStartOffset, splitEndOffset - splitStartOffset, pages));
                i = splitEndOffset;
                lookahead.unload();
            }
            return splits;
        }

        private static List<Page> breakSplitIntoPages(final long splitStartOffset,
                                                      final long splitEndOffset,
                                                      final int pageLength,
                                                      final MemorySegment memorySegment,
                                                      final boolean enableChecks) {
            List<Page> pages = new ArrayList<>();
            for (long i = splitStartOffset; i < splitEndOffset;) {
                long pageStartOffset = i;
                long pageEndOffset = Math.min(splitEndOffset, pageStartOffset + pageLength); // not inclusive
                if (pageEndOffset == splitEndOffset) {
                    pages.add(new Page(pageStartOffset, pageEndOffset - pageStartOffset));
                    break;
                }
                // Look past the end offset to find next '\n' till we reach the end of split
                long lookaheadLength = Math.min(MAX_STR_LEN, splitEndOffset - i);
                MemorySegment lookahead = memorySegment.asSlice(pageEndOffset, lookaheadLength);
                ByteBuffer bb = lookahead.asByteBuffer();
                // Find next offset which has either '\n' or the end of split
                LineMetadata lineMetadata = findNextOccurrenceOfNewLine(bb, (int) lookaheadLength, 0);
                pageEndOffset += lineMetadata.offset;
                if (enableChecks &&
                        memorySegment.asSlice(pageEndOffset - 1, 1).asByteBuffer().get(0) != '\n') {
                    throw new IllegalStateException("Page doesn't end with NL char");
                }
                pages.add(new Page(pageStartOffset, pageEndOffset - pageStartOffset));
                i = pageEndOffset;
                lookahead.unload();
            }
            return pages;
        }

        public void compute(final String path) throws Exception {
            // Create a random access file so that we can map the contents of the file into native memory for faster access
            try (RandomAccessFile file = new RandomAccessFile(path, "r")) {
                // Create a memory segment for the entire file
                MemorySegment globalSegment = file.getChannel().map(
                        FileChannel.MapMode.READ_ONLY, 0, file.length(), Arena.global());
                long fileLength = file.length();
                // Ensure that the split length never exceeds Integer.MAX_VALUE. This is because ByteBuffers cannot
                // be larger than 2 GiB.
                int splitLength = (int) Math.min(Integer.MAX_VALUE, Math.max(PAGE_SIZE, Math.rint(fileLength * 1.0 / nThreads)));
                printDebugMessage("fileLength = %d, splitLength = %d%n", file.length(), splitLength);
                long time1 = System.nanoTime();
                // Break the file into multiple splits. One thread would process one split.
                // This routine makes sure that the splits are uniformly sized to the best extent possible.
                // Each split would either end with a '\n' character or EOF
                List<Split> splits = breakFileIntoSplits(file, splitLength, PAGE_SIZE, globalSegment, false);
                printDebugMessage("Number of splits = %d, splits = [%s]%n", splits.size(), splits);
                printDebugMessage("Splits calculation took %d ns%n", System.nanoTime() - time1);
                // consume splits in parallel using the common fork join pool
                long time = System.nanoTime();
                List<Future<?>> futures = new ArrayList<>(splits.size() * 2);
                splits
                        .forEach(split -> {
                            // process splits concurrently using a thread pool
                            futures.add(executorService.submit(() -> {
                                MemorySegment splitSegment = globalSegment.asSlice(split.offset, split.length);
                                splitSegment.load();
                                int tid = (int) Thread.currentThread().threadId();
                                byte[] currentPage = new byte[PAGE_SIZE + MAX_STR_LEN];
                                // iterate over each page in split
                                for (Page page : split.pages) {
                                    // this byte buffer should end with '\n' or EOF
                                    MemorySegment segment = globalSegment.asSlice(page.offset, page.length);
                                    MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, 0L, currentPage, 0, (int) page.length);
                                    // iterate over search results
                                    processPage(currentPage, (int) page.length, threadLocalStates[tid]);
                                    // Explicitly commented out because unload seems to take a lot of time
                                    // segment.unload();
                                }
                                mergeInternal(threadLocalStates[tid]);
                            }));
                        });
                for (Future<?> future : futures) {
                    future.get();
                }
                printDebugMessage("Aggregate took %d ns%n", (System.nanoTime() - time));
            }
        }

        private void mergeInternal(final State state) {
            state.map.forEach((k, v) -> {
                globalMap.compute(k.toString(), (ignored, agg) -> {
                    if (agg == null) {
                        agg = v;
                    }
                    else {
                        agg.merge(v);
                    }
                    return agg;
                });
            });
        }

        public Map<String, MeasurementAggregator> sort() {
            long time = System.nanoTime();
            Map<String, MeasurementAggregator> sortedMap = new TreeMap<>(globalMap);
            printDebugMessage("Tree map construction took %d ns%n", (System.nanoTime() - time));
            return sortedMap;
        }
    }

    public static class State {
        public FastHashMap2 map;

        public State() {
            this.map = new FastHashMap2(DEFAULT_HASH_TBL_SIZE);
        }

        public static class AggregationKey {
            private final byte[] station;
            private final long h1;

            public AggregationKey(final byte[] station,
                                  final long h1) {
                this.station = station;
                this.h1 = h1;
            }

            @Override
            public String toString() {
                return new String(station, UTF_8);
            }

            @Override
            public int hashCode() {
                return (int) h1;
            }

            @Override
            public boolean equals(Object other) {
                if (!(other instanceof AggregationKey)) {
                    return false;
                }
                AggregationKey sk = (AggregationKey) other;
                return station.length == sk.station.length && Arrays.mismatch(station, sk.station) < 0;
            }
        }
    }

    public static class MeasurementAggregator {
        private double min;
        private double max;
        private double sum;
        private long count;

        public MeasurementAggregator(final double min,
                                     final double max,
                                     final double sum,
                                     final long count) {
            this.min = min;
            this.max = max;
            this.sum = sum;
            this.count = count;
        }

        public String toString() {
            double min1 = round(min);
            double max1 = round(max);
            double mean = round(round(sum) / count);
            return min1 + "/" + mean + "/" + max1;
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }

        private void merge(final MeasurementAggregator m2) {
            count += m2.count;
            min = Math.min(min, m2.min);
            max = Math.max(max, m2.max);
            sum += m2.sum;
        }
    }

    public static class NumberUtils {
        public static int toDigit(final char c) {
            return DIGIT_LOOKUP[c];
        }

        public static int fastMul10(final int i) {
            return (i << 1) + (i << 3);
        }

        public static double parseDouble2(final byte[] b,
                                          final int offset,
                                          final int len) {
            try {
                char ch0 = (char) b[offset];
                char ch1 = (char) b[offset + 1];
                char ch2 = (char) b[offset + 2];
                char ch3 = len > 3 ? (char) b[offset + 3] : ' ';
                char ch4 = len > 4 ? (char) b[offset + 4] : ' ';
                if (len == 3) {
                    int decimal = toDigit(ch0);
                    double fractional = DOUBLES[toDigit(ch2)];
                    return decimal + fractional;
                }
                else if (len == 4) {
                    // -1.2 or 11.2
                    int decimal = (ch0 == '-' ? toDigit(ch1) : (fastMul10(toDigit(ch0)) + toDigit(ch1)));
                    double fractional = DOUBLES[toDigit(ch3)];
                    if (ch0 == '-') {
                        return Math.negateExact(decimal) - fractional;
                    }
                    else {
                        return decimal + fractional;
                    }
                }
                else {
                    int decimal = fastMul10(toDigit(ch1)) + toDigit(ch2);
                    double fractional = DOUBLES[toDigit(ch4)];
                    return Math.negateExact(decimal) - fractional;
                }
            }
            catch (ArrayIndexOutOfBoundsException e) {
                printDebugMessage("Array index out of bounds for string: %s%n", new String(b, 0, len));
                throw new RuntimeException(e);
            }
            catch (StringIndexOutOfBoundsException e) {
                printDebugMessage("String index out of bounds for string: %s%n", new String(b, 0, len));
                throw new RuntimeException(e);
            }
        }
    }

    // record classes
    record Measurement(byte[] station,
                       double temperature,
                       State.AggregationKey aggregationKey) {

    public Measurement(byte[] station,
                           double temperature,
                           long h1) {
            this(station,
                    temperature,
                    new State.AggregationKey(station, h1));
        }

    }

    record LineMetadata(byte[] station,
                        byte[] temperature,
                        int stationLen,
                        int temperatureLen,
                        int offset,
                        int precomputedHashCode, boolean isAscii) {
    }

    record Split(long offset, long length, List<Page> pages) {
    }

    record Page(long offset, long length) {
    }

    // A simple implementation of HashMap which only supports compute and forEach methods
    // This implementation should ideally be faster than Java's HashMap implementation because it uses open addressing (double hashing to be specific)
    // to resolve collisions.
    public static class FastHashMap2 {
        private TableEntry[] tableEntries;
        private int size;
        private int probeInterval;

        public FastHashMap2(final int size) {
            this.size = size;
            this.tableEntries = new TableEntry[size + 10];
            this.probeInterval = 1;
        }

        public void update(final Measurement m,
                           final int idx) {
            FastHashMap2.TableEntry entry = computeSlow(m.aggregationKey, idx);
            MeasurementAggregator agg = entry.aggregator;
            if (agg == null) {
                entry.aggregator = new MeasurementAggregator(m.temperature, m.temperature, m.temperature, 1L);
            }
        }

        public TableEntry computeSlow(final State.AggregationKey k2,
                                      int idx) {
            // either find the corresponding entry if it exists (update) or find an empty slot for creating a new entry (insert)
            idx = probe(idx, k2);
            TableEntry entry = tableEntries[idx];
            if (entry == null) {
                tableEntries[idx] = new TableEntry(k2, null);
            }
            return tableEntries[idx];
        }

        public void forEach(final BiConsumer<State.AggregationKey, MeasurementAggregator> action) {
            for (int i = 0; i < size; i++) {
                TableEntry entry = tableEntries[i];
                if (entry != null) {
                    action.accept(entry.key, entry.aggregator);
                }
            }
        }

        private int probe(final int idx,
                          final State.AggregationKey k2) {

            // we need to search for other slots (empty/non-empty)
            // update curIdx to the next slot
            int nextIdx = idx;
            nextIdx = (nextIdx + probeInterval) & (size - 1);

            // iterate until we find a slot which meets any of the following criteria
            // - slot is empty
            // - slot is non-empty but
            // - h1 doesn't match with key (or)
            // - h1 matches but h2 doesn't match with key (or)
            // - h1 and h2 match but station name doesn't match
            while (nextIdx != idx) {
                if (tableEntries[nextIdx] == null ||
                        tableEntries[nextIdx].key.h1 != k2.h1 ||
                        tableEntries[nextIdx].key.station.length != k2.station.length ||
                        !tableEntries[nextIdx].key.equals(k2)) {
                    break;
                }
                nextIdx = (nextIdx + probeInterval) & (size - 1);
            }
            if (nextIdx == idx) {
                throw new IllegalStateException("Probe failed because we can't find slot for key");
            }
            return nextIdx;
        }

        public static class TableEntry {
            State.AggregationKey key;
            MeasurementAggregator aggregator;

            public TableEntry(final State.AggregationKey key,
                              final MeasurementAggregator aggregator) {
                this.key = key;
                this.aggregator = aggregator;
            }
        }

    }
}
