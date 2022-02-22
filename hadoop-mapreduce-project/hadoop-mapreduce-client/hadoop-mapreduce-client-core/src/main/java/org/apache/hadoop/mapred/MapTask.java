/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.mapred;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystem.Statistics;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.IFile.Writer;
import org.apache.hadoop.mapred.Merger.Segment;
import org.apache.hadoop.mapred.SortedRanges.SkipRangeIterator;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskCounter;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormatCounter;
import org.apache.hadoop.mapreduce.lib.map.WrappedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormatCounter;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitIndex;
import org.apache.hadoop.mapreduce.task.MapContextImpl;
import org.apache.hadoop.mapreduce.CryptoUtils;
import org.apache.hadoop.util.IndexedSortable;
import org.apache.hadoop.util.IndexedSorter;
import org.apache.hadoop.util.Progress;
import org.apache.hadoop.util.QuickSort;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringInterner;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A Map task. */
@InterfaceAudience.LimitedPrivate({"MapReduce"})
@InterfaceStability.Unstable
public class MapTask extends Task {
    /**
     * The size of each record in the index file for the map-outputs.
     */
    public static final int MAP_OUTPUT_INDEX_RECORD_LENGTH = 24;

    // The minimum permissions needed for a shuffle output file.
    private static final FsPermission SHUFFLE_OUTPUT_PERM =
            new FsPermission((short) 0640);

    private TaskSplitIndex splitMetaInfo = new TaskSplitIndex();
    private final static int APPROX_HEADER_LENGTH = 150;

    private static final Logger LOG =
            LoggerFactory.getLogger(MapTask.class.getName());

    private Progress mapPhase;
    private Progress sortPhase;

    {   // set phase for this task
        setPhase(TaskStatus.Phase.MAP);
        getProgress().setStatus("map");
    }

    public MapTask() {
        super();
    }

    public MapTask(String jobFile, TaskAttemptID taskId,
                   int partition, TaskSplitIndex splitIndex,
                   int numSlotsRequired) {
        super(jobFile, taskId, partition, numSlotsRequired);
        this.splitMetaInfo = splitIndex;
    }

    @Override
    public boolean isMapTask() {
        return true;
    }

    @Override
    public void localizeConfiguration(JobConf conf)
            throws IOException {
        super.localizeConfiguration(conf);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        if (isMapOrReduce()) {
            splitMetaInfo.write(out);
            splitMetaInfo = null;
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        if (isMapOrReduce()) {
            splitMetaInfo.readFields(in);
        }
    }

    /**
     * This class wraps the user's record reader to update the counters and progress
     * as records are read.
     * @param <K>
     * @param <V>
     */
    class TrackedRecordReader<K, V>
            implements RecordReader<K, V> {
        private RecordReader<K, V> rawIn;
        private Counters.Counter fileInputByteCounter;
        private Counters.Counter inputRecordCounter;
        private TaskReporter reporter;
        private long bytesInPrev = -1;
        private long bytesInCurr = -1;
        private final List<Statistics> fsStats;

        TrackedRecordReader(TaskReporter reporter, JobConf job)
                throws IOException {
            inputRecordCounter = reporter.getCounter(TaskCounter.MAP_INPUT_RECORDS);
            fileInputByteCounter = reporter.getCounter(FileInputFormatCounter.BYTES_READ);
            this.reporter = reporter;

            List<Statistics> matchedStats = null;
            if (this.reporter.getInputSplit() instanceof FileSplit) {
                matchedStats = getFsStatistics(((FileSplit) this.reporter
                        .getInputSplit()).getPath(), job);
            }
            fsStats = matchedStats;

            bytesInPrev = getInputBytes(fsStats);
            rawIn = job.getInputFormat().getRecordReader(reporter.getInputSplit(),
                    job, reporter);
            bytesInCurr = getInputBytes(fsStats);
            fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
        }

        public K createKey() {
            return rawIn.createKey();
        }

        public V createValue() {
            return rawIn.createValue();
        }

        public synchronized boolean next(K key, V value)
                throws IOException {
            boolean ret = moveToNext(key, value);
            if (ret) {
                incrCounters();
            }
            return ret;
        }

        protected void incrCounters() {
            inputRecordCounter.increment(1);
        }

        protected synchronized boolean moveToNext(K key, V value)
                throws IOException {
            bytesInPrev = getInputBytes(fsStats);
            boolean ret = rawIn.next(key, value);
            bytesInCurr = getInputBytes(fsStats);
            fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
            reporter.setProgress(getProgress());
            return ret;
        }

        public long getPos() throws IOException {
            return rawIn.getPos();
        }

        public void close() throws IOException {
            bytesInPrev = getInputBytes(fsStats);
            rawIn.close();
            bytesInCurr = getInputBytes(fsStats);
            fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
        }

        public float getProgress() throws IOException {
            return rawIn.getProgress();
        }

        TaskReporter getTaskReporter() {
            return reporter;
        }

        private long getInputBytes(List<Statistics> stats) {
            if (stats == null) return 0;
            long bytesRead = 0;
            for (Statistics stat : stats) {
                bytesRead = bytesRead + stat.getBytesRead();
            }
            return bytesRead;
        }
    }

    /**
     * This class skips the records based on the failed ranges from previous
     * attempts.
     */
    class SkippingRecordReader<K, V> extends TrackedRecordReader<K, V> {
        private SkipRangeIterator skipIt;
        private SequenceFile.Writer skipWriter;
        private boolean toWriteSkipRecs;
        private TaskUmbilicalProtocol umbilical;
        private Counters.Counter skipRecCounter;
        private long recIndex = -1;

        SkippingRecordReader(TaskUmbilicalProtocol umbilical,
                             TaskReporter reporter, JobConf job) throws IOException {
            super(reporter, job);
            this.umbilical = umbilical;
            this.skipRecCounter = reporter.getCounter(TaskCounter.MAP_SKIPPED_RECORDS);
            this.toWriteSkipRecs = toWriteSkipRecs() &&
                    SkipBadRecords.getSkipOutputPath(conf) != null;
            skipIt = getSkipRanges().skipRangeIterator();
        }

        public synchronized boolean next(K key, V value)
                throws IOException {
            if (!skipIt.hasNext()) {
                LOG.warn("Further records got skipped.");
                return false;
            }
            boolean ret = moveToNext(key, value);
            long nextRecIndex = skipIt.next();
            long skip = 0;
            while (recIndex < nextRecIndex && ret) {
                if (toWriteSkipRecs) {
                    writeSkippedRec(key, value);
                }
                ret = moveToNext(key, value);
                skip++;
            }
            //close the skip writer once all the ranges are skipped
            if (skip > 0 && skipIt.skippedAllRanges() && skipWriter != null) {
                skipWriter.close();
            }
            skipRecCounter.increment(skip);
            reportNextRecordRange(umbilical, recIndex);
            if (ret) {
                incrCounters();
            }
            return ret;
        }

        protected synchronized boolean moveToNext(K key, V value)
                throws IOException {
            recIndex++;
            return super.moveToNext(key, value);
        }

        @SuppressWarnings("unchecked")
        private void writeSkippedRec(K key, V value) throws IOException {
            if (skipWriter == null) {
                Path skipDir = SkipBadRecords.getSkipOutputPath(conf);
                Path skipFile = new Path(skipDir, getTaskID().toString());
                skipWriter =
                        SequenceFile.createWriter(
                                skipFile.getFileSystem(conf), conf, skipFile,
                                (Class<K>) createKey().getClass(),
                                (Class<V>) createValue().getClass(),
                                CompressionType.BLOCK, getTaskReporter());
            }
            skipWriter.append(key, value);
        }
    }

    @Override
    public void run(final JobConf job, final TaskUmbilicalProtocol umbilical)
            throws IOException, ClassNotFoundException, InterruptedException {
        this.umbilical = umbilical;

        if (isMapTask()) {
            // If there are no reducers then there won't be any sort. Hence the map
            // phase will govern the entire attempt's progress.
            if (conf.getNumReduceTasks() == 0) {
                /**
                 * 如果本作业中没有ReduceTask，那Mapper的输出就不需要排序
                 * 所以Map操作的工作量就是这个任务的全部工作量
                 */
                mapPhase = getProgress().addPhase("map", 1.0f);
            } else {
                // If there are reducers then the entire attempt's progress will be
                // split between the map phase (67%) and the sort phase (33%).
                /**
                 * 否则，如果有Reducer，那就需要对输出进行排序；
                 */
                mapPhase = getProgress().addPhase("map", 0.667f); // Map阶段的工作量算2/3
                sortPhase = getProgress().addPhase("sort", 0.333f); // 输出排序阶段的工作量算1/3
            }
        }
        TaskReporter reporter = startReporter(umbilical);

        boolean useNewApi = job.getUseNewMapper(); // 是新API还是老API
        initialize(job, getJobID(), reporter, useNewApi); // MapTask的初始化

        // check if it is a cleanupJobTask
        if (jobCleanup) {
            runJobCleanupTask(umbilical, reporter); // 如果只是要求作业清扫，则清扫一下就返回
            return;
        }
        if (jobSetup) {
            runJobSetupTask(umbilical, reporter); // 如果只是为了任务执行"打前站"，那就准备一下就返回
            return;
        }
        if (taskCleanup) {
            runTaskCleanupTask(umbilical, reporter); // 如果值要任务清扫，那清扫一下就返回
            return;
        }

        if (useNewApi) { // 如果采用新的API的mapper
            runNewMapper(job, splitMetaInfo, umbilical, reporter);
        } else { // 采用老的API的mapper
            runOldMapper(job, splitMetaInfo, umbilical, reporter);
        }
        done(umbilical, reporter); // 通知MRAppMaster，本任务已经完成
    }

    public Progress getSortPhase() {
        return sortPhase;
    }

    @SuppressWarnings("unchecked")
    private <T> T getSplitDetails(Path file, long offset)
            throws IOException {
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream inFile = fs.open(file);
        inFile.seek(offset);
        String className = StringInterner.weakIntern(Text.readString(inFile));
        Class<T> cls;
        try {
            cls = (Class<T>) conf.getClassByName(className);
        } catch (ClassNotFoundException ce) {
            IOException wrap = new IOException("Split class " + className +
                    " not found");
            wrap.initCause(ce);
            throw wrap;
        }
        SerializationFactory factory = new SerializationFactory(conf);
        Deserializer<T> deserializer =
                (Deserializer<T>) factory.getDeserializer(cls);
        deserializer.open(inFile);
        T split = deserializer.deserialize(null);
        long pos = inFile.getPos();
        getCounters().findCounter(
                TaskCounter.SPLIT_RAW_BYTES).increment(pos - offset);
        inFile.close();
        return split;
    }

    @SuppressWarnings("unchecked")
    private <KEY, VALUE> MapOutputCollector<KEY, VALUE>
    createSortingCollector(JobConf job, TaskReporter reporter)
            throws IOException, ClassNotFoundException {
        MapOutputCollector.Context context =
                new MapOutputCollector.Context(this, job, reporter);

        Class<?>[] collectorClasses = job.getClasses(
                JobContext.MAP_OUTPUT_COLLECTOR_CLASS_ATTR, MapOutputBuffer.class); // 如果未加设置，就默认为MapOutputBuffer.class
        int remainingCollectors = collectorClasses.length;
        Exception lastException = null;
        for (Class clazz : collectorClasses) { // 逐一试验所设置的collectorClasses
            try {
                if (!MapOutputCollector.class.isAssignableFrom(clazz)) {
                    throw new IOException("Invalid output collector class: " + clazz.getName() +
                            " (does not implement MapOutputCollector)");
                }
                Class<? extends MapOutputCollector> subclazz =
                        clazz.asSubclass(MapOutputCollector.class); // 必须实现mappOuputCollector界面
                LOG.debug("Trying map output collector class: " + subclazz.getName());
                MapOutputCollector<KEY, VALUE> collector =
                        ReflectionUtils.newInstance(subclazz, job); // 创建collector对象
                collector.init(context); // 初始化，实际上是MapTask.MapOutputBuffer的初始化
                LOG.info("Map output collector class = " + collector.getClass().getName());
                return collector; // 如果过程中未发生异常，那就成功了
            } catch (Exception e) {
                String msg = "Unable to initialize MapOutputCollector " + clazz.getName();
                if (--remainingCollectors > 0) {
                    msg += " (" + remainingCollectors + " more collector(s) to try)";
                }
                lastException = e;
                LOG.warn(msg, e);
            }
        }

        if (lastException != null) {
            throw new IOException("Initialization of all the collectors failed. " +
                    "Error in last collector was:" + lastException.toString(),
                    lastException);
        } else {
            throw new IOException("Initialization of all the collectors failed.");
        }
    }

    @SuppressWarnings("unchecked")
    private <INKEY, INVALUE, OUTKEY, OUTVALUE>
    void runOldMapper(final JobConf job,
                      final TaskSplitIndex splitIndex,
                      final TaskUmbilicalProtocol umbilical,
                      TaskReporter reporter
    ) throws IOException, InterruptedException,
            ClassNotFoundException {
        InputSplit inputSplit = getSplitDetails(new Path(splitIndex.getSplitLocation()),
                splitIndex.getStartOffset()); // 获取关系本任务输入split的信息，包括文件路径和起点

        updateJobWithSplit(job, inputSplit); // 设置inputSplit
        reporter.setInputSplit(inputSplit); // reporter应报告这个Split的进度
        /**
         * 从数据源读入时如遇错误是否跳过错误继续往下进行
         */
        RecordReader<INKEY, INVALUE> in = isSkipping() ?
                new SkippingRecordReader<INKEY, INVALUE>(umbilical, reporter, job) :
                new TrackedRecordReader<INKEY, INVALUE>(reporter, job);
        job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());
        int numReduceTasks = conf.getNumReduceTasks(); // 从JobConf中获取Reducer数量
        LOG.info("numReduceTasks: " + numReduceTasks);
        MapOutputCollector<OUTKEY, OUTVALUE> collector = null;
        if (numReduceTasks > 0) { // 有Reducer，Mapper的输出应该收集后供Reducer使用
            collector = createSortingCollector(job, reporter);
        } else { // 没有Reducer，Mapper的输出直接就是结果
            collector = new DirectMapOutputCollector<OUTKEY, OUTVALUE>();
            MapOutputCollector.Context context =
                    new MapOutputCollector.Context(this, job, reporter);
            collector.init(context);
        }
        /**
         * 从JobConf中获取MapRunnable的类型并加以创建，这次是PipeMapRunner
         * 创建PipeMapRunner对象时会调用其configure()函数
         */
        MapRunnable<INKEY, INVALUE, OUTKEY, OUTVALUE> runner =
                ReflectionUtils.newInstance(job.getMapRunnerClass(), job);

        try {
            // 执行PipeMapRunner.run()
            runner.run(in, new OldOutputCollector(collector, conf), reporter);
            mapPhase.complete();
            // start the sort phase only if there are reducers
            if (numReduceTasks > 0) {
                setPhase(TaskStatus.Phase.SORT);
            }
            statusUpdate(umbilical);
            collector.flush();

            in.close();
            in = null;

            collector.close();
            collector = null;
        } finally {
            closeQuietly(in);
            closeQuietly(collector);
        }
    }

    /**
     * Update the job with details about the file split
     * @param job the job configuration to update
     * @param inputSplit the file split
     */
    private void updateJobWithSplit(final JobConf job, InputSplit inputSplit) {
        if (inputSplit instanceof FileSplit) {
            FileSplit fileSplit = (FileSplit) inputSplit;
            job.set(JobContext.MAP_INPUT_FILE, fileSplit.getPath().toString());
            job.setLong(JobContext.MAP_INPUT_START, fileSplit.getStart());
            job.setLong(JobContext.MAP_INPUT_PATH, fileSplit.getLength());
        }
        LOG.info("Processing split: " + inputSplit);
    }

    static class NewTrackingRecordReader<K, V>
            extends org.apache.hadoop.mapreduce.RecordReader<K, V> {
        private final org.apache.hadoop.mapreduce.RecordReader<K, V> real;
        private final org.apache.hadoop.mapreduce.Counter inputRecordCounter;
        private final org.apache.hadoop.mapreduce.Counter fileInputByteCounter;
        private final TaskReporter reporter;
        private final List<Statistics> fsStats;

        NewTrackingRecordReader(org.apache.hadoop.mapreduce.InputSplit split,
                                org.apache.hadoop.mapreduce.InputFormat<K, V> inputFormat,
                                TaskReporter reporter,
                                org.apache.hadoop.mapreduce.TaskAttemptContext taskContext)
                throws InterruptedException, IOException {
            this.reporter = reporter;
            this.inputRecordCounter = reporter
                    .getCounter(TaskCounter.MAP_INPUT_RECORDS);
            this.fileInputByteCounter = reporter
                    .getCounter(FileInputFormatCounter.BYTES_READ);

            List<Statistics> matchedStats = null;
            if (split instanceof org.apache.hadoop.mapreduce.lib.input.FileSplit) {
                matchedStats = getFsStatistics(((org.apache.hadoop.mapreduce.lib.input.FileSplit) split)
                        .getPath(), taskContext.getConfiguration());
            }
            fsStats = matchedStats;

            long bytesInPrev = getInputBytes(fsStats);
            this.real = inputFormat.createRecordReader(split, taskContext); // TextInputFormat.createRecordReader(InputSplit split,TaskAttemptContext context)
            long bytesInCurr = getInputBytes(fsStats);
            fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
        }

        @Override
        public void close() throws IOException {
            long bytesInPrev = getInputBytes(fsStats);
            real.close();
            long bytesInCurr = getInputBytes(fsStats);
            fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
        }

        @Override
        public K getCurrentKey() throws IOException, InterruptedException {
            return real.getCurrentKey();
        }

        @Override
        public V getCurrentValue() throws IOException, InterruptedException {
            return real.getCurrentValue();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return real.getProgress();
        }

        @Override
        public void initialize(org.apache.hadoop.mapreduce.InputSplit split,
                               org.apache.hadoop.mapreduce.TaskAttemptContext context
        ) throws IOException, InterruptedException {
            long bytesInPrev = getInputBytes(fsStats);
            real.initialize(split, context); // ==LineRecordReader.initialize(split,context)
            long bytesInCurr = getInputBytes(fsStats);
            fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            long bytesInPrev = getInputBytes(fsStats);
            boolean result = real.nextKeyValue(); // LineRecordReader.nextKeyValue()
            long bytesInCurr = getInputBytes(fsStats);
            if (result) {
                inputRecordCounter.increment(1);
            }
            fileInputByteCounter.increment(bytesInCurr - bytesInPrev);
            reporter.setProgress(getProgress());
            return result; // 返回true或者false使mapper.run()中while语句继续或停止循环
        }

        private long getInputBytes(List<Statistics> stats) {
            if (stats == null) return 0;
            long bytesRead = 0;
            for (Statistics stat : stats) {
                bytesRead = bytesRead + stat.getBytesRead();
            }
            return bytesRead;
        }
    }

    /**
     * Since the mapred and mapreduce Partitioners don't share a common interface
     * (JobConfigurable is deprecated and a subtype of mapred.Partitioner), the
     * partitioner lives in Old/NewOutputCollector. Note that, for map-only jobs,
     * the configured partitioner should not be called. It's common for
     * partitioners to compute a result mod numReduces, which causes a div0 error
     */
    private static class OldOutputCollector<K, V> implements OutputCollector<K, V> {
        private final Partitioner<K, V> partitioner;
        private final MapOutputCollector<K, V> collector;
        private final int numPartitions;

        @SuppressWarnings("unchecked")
        OldOutputCollector(MapOutputCollector<K, V> collector, JobConf conf) {
            numPartitions = conf.getNumReduceTasks();
            if (numPartitions > 1) {
                partitioner = (Partitioner<K, V>)
                        ReflectionUtils.newInstance(conf.getPartitionerClass(), conf);
            } else {
                partitioner = new Partitioner<K, V>() {
                    @Override
                    public void configure(JobConf job) {
                    }

                    @Override
                    public int getPartition(K key, V value, int numPartitions) {
                        return numPartitions - 1;
                    }
                };
            }
            this.collector = collector;
        }

        @Override
        public void collect(K key, V value) throws IOException {
            try {
                collector.collect(key, value,
                        partitioner.getPartition(key, value, numPartitions));
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("interrupt exception", ie);
            }
        }
    }

    private class NewDirectOutputCollector<K, V>
            extends org.apache.hadoop.mapreduce.RecordWriter<K, V> {
        private final org.apache.hadoop.mapreduce.RecordWriter out;

        private final TaskReporter reporter;

        private final Counters.Counter mapOutputRecordCounter;
        private final Counters.Counter fileOutputByteCounter;
        private final List<Statistics> fsStats;

        @SuppressWarnings("unchecked")
        NewDirectOutputCollector(MRJobConfig jobContext,
                                 JobConf job, TaskUmbilicalProtocol umbilical, TaskReporter reporter)
                throws IOException, ClassNotFoundException, InterruptedException {
            this.reporter = reporter;
            mapOutputRecordCounter = reporter
                    .getCounter(TaskCounter.MAP_OUTPUT_RECORDS);
            fileOutputByteCounter = reporter
                    .getCounter(FileOutputFormatCounter.BYTES_WRITTEN);

            List<Statistics> matchedStats = null;
            if (outputFormat instanceof org.apache.hadoop.mapreduce.lib.output.FileOutputFormat) {
                matchedStats = getFsStatistics(org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
                        .getOutputPath(taskContext), taskContext.getConfiguration());
            }
            fsStats = matchedStats;

            long bytesOutPrev = getOutputBytes(fsStats);
            out = outputFormat.getRecordWriter(taskContext);
            long bytesOutCurr = getOutputBytes(fsStats);
            fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void write(K key, V value)
                throws IOException, InterruptedException {
            reporter.progress();
            long bytesOutPrev = getOutputBytes(fsStats);
            out.write(key, value);
            long bytesOutCurr = getOutputBytes(fsStats);
            fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
            mapOutputRecordCounter.increment(1);
        }

        @Override
        public void close(TaskAttemptContext context)
                throws IOException, InterruptedException {
            reporter.progress();
            if (out != null) {
                long bytesOutPrev = getOutputBytes(fsStats);
                out.close(context);
                long bytesOutCurr = getOutputBytes(fsStats);
                fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
            }
        }

        private long getOutputBytes(List<Statistics> stats) {
            if (stats == null) return 0;
            long bytesWritten = 0;
            for (Statistics stat : stats) {
                bytesWritten = bytesWritten + stat.getBytesWritten();
            }
            return bytesWritten;
        }
    }

    private class NewOutputCollector<K, V>
            extends org.apache.hadoop.mapreduce.RecordWriter<K, V> {
        private final MapOutputCollector<K, V> collector;
        private final org.apache.hadoop.mapreduce.Partitioner<K, V> partitioner; // 负责Mapper输出的分区
        private final int partitions; // 分发目标的个数，即Reducer的个数


        @SuppressWarnings("unchecked")
        NewOutputCollector(org.apache.hadoop.mapreduce.JobContext jobContext,
                           JobConf job,
                           TaskUmbilicalProtocol umbilical,
                           TaskReporter reporter
        ) throws IOException, ClassNotFoundException {
            collector = createSortingCollector(job, reporter); // 创建通向排序阶段的Collector
            partitions = jobContext.getNumReduceTasks(); // 有几个Reducer，就有几个partition
            if (partitions > 1) { // 如果有多个partition
                partitioner = (org.apache.hadoop.mapreduce.Partitioner<K, V>)
                        ReflectionUtils.newInstance(jobContext.getPartitionerClass(), job); // 应该是对抽象类Partitioner的某种扩充，如果未加设定就默认hashPartitioner,创建用户设置的Partitioner
            } else { // 只有一个partition，就动态扩充抽象类Partitioner
                partitioner = new org.apache.hadoop.mapreduce.Partitioner<K, V>() {
                    @Override
                    public int getPartition(K key, V value, int numPartitions) {
                        return partitions - 1;
                    }
                };
            }
        }

        @Override
        public void write(K key, V value) throws IOException, InterruptedException {
            collector.collect(key, value,
                    partitioner.getPartition(key, value, partitions));
        }

        @Override
        public void close(TaskAttemptContext context
        ) throws IOException, InterruptedException {
            try {
                collector.flush();
            } catch (ClassNotFoundException cnf) {
                throw new IOException("can't find class ", cnf);
            }
            collector.close();
        }
    }

    /**
     *
     * @param job
     * @param splitIndex 表明这个Map任务的输入来自输入文件中的哪一个Split
     * @param umbilical
     * @param reporter
     * @param <INKEY>
     * @param <INVALUE>
     * @param <OUTKEY>
     * @param <OUTVALUE>
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    @SuppressWarnings("unchecked")
    private <INKEY, INVALUE, OUTKEY, OUTVALUE>
    void runNewMapper(final JobConf job,
                      final TaskSplitIndex splitIndex,
                      final TaskUmbilicalProtocol umbilical,
                      TaskReporter reporter
    ) throws IOException, ClassNotFoundException,
            InterruptedException {
        // make a task context so we can get the classes
        org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
                new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(job,
                        getTaskID(),
                        reporter);
        // make a mapper，这就是用户给定的Mapper类，如果没有给定就采用默认的Mapper.class
        org.apache.hadoop.mapreduce.Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE> mapper =
                (org.apache.hadoop.mapreduce.Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>)
                        ReflectionUtils.newInstance(taskContext.getMapperClass(), job);
        // make the input format
        org.apache.hadoop.mapreduce.InputFormat<INKEY, INVALUE> inputFormat =
                (org.apache.hadoop.mapreduce.InputFormat<INKEY, INVALUE>)
                        ReflectionUtils.newInstance(taskContext.getInputFormatClass(), job);
        // rebuild the input split
        org.apache.hadoop.mapreduce.InputSplit split = null;
        /**
         * Mapper的输入文件一般是分片的，每个MapTask一片，通常就是一个数据块
         * 根据splitIndex找到输入文件（是HDFS文件）中的某个块
         * HDFS的文件的每个块都是宿主机文件系统中的一个文件
         */
        split = getSplitDetails(new Path(splitIndex.getSplitLocation()),
                splitIndex.getStartOffset());
        LOG.info("Processing split: " + split);
        /**
         * 用于从输入文件读入
         * 带Tracking功能的RecordReader，这个对象将为Mapper提供输入
         */
        org.apache.hadoop.mapreduce.RecordReader<INKEY, INVALUE> input =
                new NewTrackingRecordReader<INKEY, INVALUE>
                        (split, inputFormat, reporter, taskContext);
        /**
         * 碰到损坏的记录是否跳过
         */
        job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());
        org.apache.hadoop.mapreduce.RecordWriter output = null;

        // get an output object

        if (job.getNumReduceTasks() == 0) {
            /**
             * 如果不用Reducer，则Mapper直接输出结果
             * 就是整个作业的输出
             */
            output =
                    new NewDirectOutputCollector(taskContext, job, umbilical, reporter);
        } else {
            /**
             * 一般都要将mapper的输出收集起来供reducer处理
             * 创建Mapper的输出RecordWriter，包括collector和partitioner
             */
            output = new NewOutputCollector(taskContext, job, umbilical, reporter);
        }

        org.apache.hadoop.mapreduce.MapContext<INKEY, INVALUE, OUTKEY, OUTVALUE>
                mapContext =
                new MapContextImpl<INKEY, INVALUE, OUTKEY, OUTVALUE>(job, getTaskID(),
                        input, output,
                        committer,
                        reporter, split); // 创建用于Mapper的Context

        org.apache.hadoop.mapreduce.Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context
                mapperContext =
                new WrappedMapper<INKEY, INVALUE, OUTKEY, OUTVALUE>().getMapContext(
                        mapContext); // z

        try {
            input.initialize(split, mapperContext); // 输入流的初始化
            mapper.run(mapperContext); // 开始执行用户指定的Mapper
            mapPhase.complete();
            setPhase(TaskStatus.Phase.SORT); // Map阶段结束，进入排序阶段
            statusUpdate(umbilical);
            input.close();
            input = null;
            output.close(mapperContext);
            output = null;
        } finally {
            closeQuietly(input);
            closeQuietly(output, mapperContext);
        }
    }

    class DirectMapOutputCollector<K, V>
            implements MapOutputCollector<K, V> {

        private RecordWriter<K, V> out = null;

        private TaskReporter reporter = null;

        private Counters.Counter mapOutputRecordCounter;
        private Counters.Counter fileOutputByteCounter;
        private List<Statistics> fsStats;

        public DirectMapOutputCollector() {
        }

        @SuppressWarnings("unchecked")
        public void init(MapOutputCollector.Context context
        ) throws IOException, ClassNotFoundException {
            this.reporter = context.getReporter();
            JobConf job = context.getJobConf();
            String finalName = getOutputName(getPartition());
            FileSystem fs = FileSystem.get(job);

            OutputFormat<K, V> outputFormat = job.getOutputFormat();
            mapOutputRecordCounter = reporter.getCounter(TaskCounter.MAP_OUTPUT_RECORDS);

            fileOutputByteCounter = reporter
                    .getCounter(FileOutputFormatCounter.BYTES_WRITTEN);

            List<Statistics> matchedStats = null;
            if (outputFormat instanceof FileOutputFormat) {
                matchedStats = getFsStatistics(FileOutputFormat.getOutputPath(job), job);
            }
            fsStats = matchedStats;

            long bytesOutPrev = getOutputBytes(fsStats);
            out = job.getOutputFormat().getRecordWriter(fs, job, finalName, reporter);
            long bytesOutCurr = getOutputBytes(fsStats);
            fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
        }

        public void close() throws IOException {
            if (this.out != null) {
                long bytesOutPrev = getOutputBytes(fsStats);
                out.close(this.reporter);
                long bytesOutCurr = getOutputBytes(fsStats);
                fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
            }

        }

        public void flush() throws IOException, InterruptedException,
                ClassNotFoundException {
        }

        public void collect(K key, V value, int partition) throws IOException {
            reporter.progress();
            long bytesOutPrev = getOutputBytes(fsStats);
            out.write(key, value);
            long bytesOutCurr = getOutputBytes(fsStats);
            fileOutputByteCounter.increment(bytesOutCurr - bytesOutPrev);
            mapOutputRecordCounter.increment(1);
        }

        private long getOutputBytes(List<Statistics> stats) {
            if (stats == null) return 0;
            long bytesWritten = 0;
            for (Statistics stat : stats) {
                bytesWritten = bytesWritten + stat.getBytesWritten();
            }
            return bytesWritten;
        }
    }

    @InterfaceAudience.LimitedPrivate({"MapReduce"})
    @InterfaceStability.Unstable
    public static class MapOutputBuffer<K extends Object, V extends Object>
            implements MapOutputCollector<K, V>, IndexedSortable {
        private int partitions;  // 输出数据partition的总数，即Reducer的总数
        private JobConf job;
        private TaskReporter reporter;  // 用于向上级报告任务进展
        private Class<K> keyClass;
        private Class<V> valClass;
        private RawComparator<K> comparator;
        private SerializationFactory serializationFactory;
        private Serializer<K> keySerializer;
        private Serializer<V> valSerializer;
        private CombinerRunner<K, V> combinerRunner; // Combine环节的Runner对象
        private CombineOutputCollector<K, V> combineCollector; // Combine环节的collector

        // Compression for map-outputs
        private CompressionCodec codec;

        // k/v accounting
        private IntBuffer kvmeta; // metadata overlay on backing store
        int kvstart;            // marks origin of spill metadata
        int kvend;              // marks end of spill metadata
        int kvindex;            // marks end of fully serialized records

        int equator;            // marks origin of meta/serialization
        int bufstart;           // marks beginning of spill
        int bufend;             // marks beginning of collectable
        int bufmark;            // marks end of record
        int bufindex;           // marks end of collected
        int bufvoid;            // marks the point where we should stop
        // reading at the end of the buffer

        byte[] kvbuffer;        // main output buffer ,显然这是个KV对缓冲区
        private final byte[] b0 = new byte[0]; // 用于串行化时的临时缓冲，类似于草稿纸

        private static final int VALSTART = 0;         // val offset in acct
        private static final int KEYSTART = 1;         // key offset in acct
        private static final int PARTITION = 2;        // partition offset in acct
        private static final int VALLEN = 3;           // length of value
        private static final int NMETA = 4;            // num meta ints
        private static final int METASIZE = NMETA * 4; // size in bytes

        // spill accounting
        private int maxRec;
        private int softLimit;
        boolean spillInProgress;
        ;
        int bufferRemaining;
        volatile Throwable sortSpillException = null;

        int numSpills = 0;
        private int minSpillsForCombine;
        private IndexedSorter sorter; // 用于排序的Sorter对象
        final ReentrantLock spillLock = new ReentrantLock();
        final Condition spillDone = spillLock.newCondition();
        final Condition spillReady = spillLock.newCondition();
        final BlockingBuffer bb = new BlockingBuffer(); // 实际上是个输出流，提供将缓冲区用于K/V知道串行化和写入的操作方法
        volatile boolean spillThreadRunning = false;
        final SpillThread spillThread = new SpillThread(); // 专门将缓冲区内容写入Spill文件的线程

        private FileSystem rfs;

        // Counters
        private Counters.Counter mapOutputByteCounter;
        private Counters.Counter mapOutputRecordCounter;
        private Counters.Counter fileOutputByteCounter;

        final ArrayList<SpillRecord> indexCacheList =
                new ArrayList<SpillRecord>();
        private int totalIndexCacheMemory;
        private int indexCacheMemoryLimit;
        private static final int INDEX_CACHE_MEMORY_LIMIT_DEFAULT = 1024 * 1024;

        private MapTask mapTask;
        private MapOutputFile mapOutputFile;
        private Progress sortPhase;
        private Counters.Counter spilledRecordsCounter;

        public MapOutputBuffer() {
        }

        @SuppressWarnings("unchecked")
        public void init(MapOutputCollector.Context context
        ) throws IOException, ClassNotFoundException {
            job = context.getJobConf();
            reporter = context.getReporter(); // 用来向上级报告进度
            mapTask = context.getMapTask();
            mapOutputFile = mapTask.getMapOutputFile(); // 必须是对抽象类MapOutputFile的某种扩充，默认为MROutputFiles
            sortPhase = mapTask.getSortPhase();
            spilledRecordsCounter = reporter.getCounter(TaskCounter.SPILLED_RECORDS);
            partitions = job.getNumReduceTasks(); // 有几个Reducer就分为几个Partition
            rfs = ((LocalFileSystem) FileSystem.getLocal(job)).getRaw(); // Spill文件所在的目录

            //sanity checks
            final float spillper =
                    job.getFloat(JobContext.MAP_SORT_SPILL_PERCENT, (float) 0.8); // spill门槛值，百分比，默认为80%
            final int sortmb = job.getInt(MRJobConfig.IO_SORT_MB,
                    MRJobConfig.DEFAULT_IO_SORT_MB); // 用不排序的缓冲区大小，默认为100mb
            indexCacheMemoryLimit = job.getInt(JobContext.INDEX_CACHE_MEMORY_LIMIT,
                    INDEX_CACHE_MEMORY_LIMIT_DEFAULT);
            if (spillper > (float) 1.0 || spillper <= (float) 0.0) {
                throw new IOException("Invalid \"" + JobContext.MAP_SORT_SPILL_PERCENT +
                        "\": " + spillper);
            }
            if ((sortmb & 0x7FF) != sortmb) {
                throw new IOException(
                        "Invalid \"" + JobContext.IO_SORT_MB + "\": " + sortmb);
            }
            sorter = ReflectionUtils.newInstance(job.getClass(
                    MRJobConfig.MAP_SORT_CLASS, QuickSort.class,
                    IndexedSorter.class), job);  // 用于排序的算法，默认为QuickSort
            // buffers and accounting
            int maxMemUsage = sortmb << 20; // 左移20位正好是1M。如果sortmb是100，maxMemUsage就是100M
            maxMemUsage -= maxMemUsage % METASIZE; // METASIZE 是16，必须是16的整数倍（但既然左移了20位，就当然是对齐的）
            kvbuffer = new byte[maxMemUsage]; // 创建物理缓冲区，缓冲区大小默认为100m
            bufvoid = kvbuffer.length; // 这个缓冲区的上界
            kvmeta = ByteBuffer.wrap(kvbuffer)       // 先将数组kvbuffer包装成一个字节缓冲区ByteBuffer，再设置好在此缓冲区中存储整数时的字节顺序（BigEndian或LittleEndia)
                    .order(ByteOrder.nativeOrder())  // 并为其建立一个用作整数缓冲区IntBuffer的视图（View），以便整数的读写。
                    .asIntBuffer();

            setEquator(0);  // 将分隔点设置在0位上
            bufstart = bufend = bufindex = equator; // 此时的ByteBuffer视图为空
            kvstart = kvend = kvindex; // IntBuffer视图也为空

            maxRec = kvmeta.capacity() / NMETA; // 最大记录数是IntBuffer的容量除以16，因为每个NMETA的大小是16
            softLimit = (int) (kvbuffer.length * spillper);
            bufferRemaining = softLimit;
            LOG.info(JobContext.IO_SORT_MB + ": " + sortmb);
            LOG.info("soft limit at " + softLimit);
            LOG.info("bufstart = " + bufstart + "; bufvoid = " + bufvoid);
            LOG.info("kvstart = " + kvstart + "; length = " + maxRec);

            // k/v serialization
            comparator = job.getOutputKeyComparator();
            keyClass = (Class<K>) job.getMapOutputKeyClass();
            valClass = (Class<V>) job.getMapOutputValueClass();
            serializationFactory = new SerializationFactory(job); // 用于KV对的串行化
            keySerializer = serializationFactory.getSerializer(keyClass);
            keySerializer.open(bb); // bb是个BlockingBuffer，将其用于K值串行化后的输出
            valSerializer = serializationFactory.getSerializer(valClass); // 用于其中V值的串行化
            valSerializer.open(bb); // 也将缓冲区bb失败BlockingBuffer，将用于V值串行化后的输出。

            // output counters
            mapOutputByteCounter = reporter.getCounter(TaskCounter.MAP_OUTPUT_BYTES);
            mapOutputRecordCounter =
                    reporter.getCounter(TaskCounter.MAP_OUTPUT_RECORDS);
            fileOutputByteCounter = reporter
                    .getCounter(TaskCounter.MAP_OUTPUT_MATERIALIZED_BYTES);

            // compression
            if (job.getCompressMapOutput()) { // 如果Mapper的输出需要压缩
                Class<? extends CompressionCodec> codecClass =
                        job.getMapOutputCompressorClass(DefaultCodec.class);
                codec = ReflectionUtils.newInstance(codecClass, job); // 创建编解码器对象
            } else {
                codec = null;
            }

            // combiner
            final Counters.Counter combineInputCounter =
                    reporter.getCounter(TaskCounter.COMBINE_INPUT_RECORDS);
            combinerRunner = CombinerRunner.create(job, getTaskID(),
                    combineInputCounter,
                    reporter, null); // 创建CombinerRunner
            if (combinerRunner != null) { // 若使用combinerRunner，就必须创建器collector
                final Counters.Counter combineOutputCounter =
                        reporter.getCounter(TaskCounter.COMBINE_OUTPUT_RECORDS);
                combineCollector = new CombineOutputCollector<K, V>(combineOutputCounter, reporter, job); // CombineOutputCollector实现OutputCollector界面，提供collect()函数
            } else {
                combineCollector = null; // 不用combinerRunner，就无须combineCollector
            }
            spillInProgress = false;
            minSpillsForCombine = job.getInt(JobContext.MAP_COMBINE_MIN_SPILLS, 3); // 如果Spill文件的数量未达到门槛值（默认为3），就无须Combine
            spillThread.setDaemon(true); //将Spill线程设置成Daemon，及端口器stdin/stdout
            spillThread.setName("SpillThread");
            spillLock.lock();
            try {
                spillThread.start();
                while (!spillThreadRunning) {
                    spillDone.await(); // 等待，确认Spill线程已在运行才返回
                }
            } catch (InterruptedException e) {
                throw new IOException("Spill thread failed to initialize", e);
            } finally {
                spillLock.unlock();
            }
            if (sortSpillException != null) {
                throw new IOException("Spill thread failed to initialize",
                        sortSpillException);
            }
        }

        /**
         * Serialize the key, value to intermediate storage.
         * When this method returns, kvindex must refer to sufficient unused
         * storage to store one METADATA.
         */
        public synchronized void collect(K key, V value, final int partition
        ) throws IOException {
            reporter.progress();
            if (key.getClass() != keyClass) {
                throw new IOException("Type mismatch in key from map: expected "
                        + keyClass.getName() + ", received "
                        + key.getClass().getName());
            }
            if (value.getClass() != valClass) {
                throw new IOException("Type mismatch in value from map: expected "
                        + valClass.getName() + ", received "
                        + value.getClass().getName());
            }
            if (partition < 0 || partition >= partitions) {
                throw new IOException("Illegal partition for " + key + " (" +
                        partition + ")");
            }
            checkSpillException();
            bufferRemaining -= METASIZE; // METASIZE = NMETA * 4，NMETA=4；
            if (bufferRemaining <= 0) { // bufferRemaining的计算已包含80%临界的考虑，剩余缓存去空间变小已地临界，需要Spill
                // start spill if the thread is not running and the soft limit has been
                // reached
                spillLock.lock();
                try {
                    do { // 一次Spill
                        if (!spillInProgress) { // 如果已经在做Spill，当然就不需要做什么了。
                            final int kvbidx = 4 * kvindex;
                            final int kvbend = 4 * kvend;
                            // serialized, unspilled bytes always lie between kvindex and
                            // bufindex, crossing the equator. Note that any void space
                            // created by a reset must be included in "used" bytes
                            final int bUsed = distanceTo(kvbidx, bufindex);
                            final boolean bufsoftlimit = bUsed >= softLimit;
                            if ((kvbend + METASIZE) % kvbuffer.length !=
                                    equator - (equator % METASIZE)) { // spill finished，reclaim space，现在Spill已经完成，需要释放空间
                                // spill finished, reclaim space      // 这个条件说明缓冲区中原来是有数据的（元数据块非空）
                                resetSpill();
                                bufferRemaining = Math.min(
                                        distanceTo(bufindex, kvbidx) - 2 * METASIZE,
                                        softLimit - bUsed) - METASIZE;
                                continue;
                            } else if (bufsoftlimit && kvindex != kvend) {
                                // spill records, if any collected; check latter, as it may
                                // be possible for metadata alignment to hit spill pcnt
                                startSpill(); // 启动spill
                                final int avgRec = (int)
                                        (mapOutputByteCounter.getCounter() /
                                                mapOutputRecordCounter.getCounter()); // 计算这些KV对的平均长度
                                // leave at least half the split buffer for serialization data
                                // ensure that kvindex >= bufindex
                                final int distkvi = distanceTo(bufindex, kvbidx);
                                final int newPos = (bufindex +
                                        Math.max(2 * METASIZE - 1,
                                                Math.min(distkvi / 2,
                                                        distkvi / (METASIZE + avgRec) * METASIZE)))
                                        % kvbuffer.length;
                                setEquator(newPos);
                                bufmark = bufindex = newPos;
                                final int serBound = 4 * kvend;
                                // bytes remaining before the lock must be held and limits
                                // checked is the minimum of three arcs: the metadata space, the
                                // serialization space, and the soft limit
                                // 重新计算bufferRemaining，softLimit为0.8 * kvbuffer.length
                                bufferRemaining = Math.min(
                                        // metadata max
                                        distanceTo(bufend, newPos),
                                        Math.min(
                                                // serialization max
                                                distanceTo(newPos, serBound),
                                                // soft limit
                                                softLimit)) - 2 * METASIZE;
                            }
                        }
                    } while (false); // 这个do while 只做一遍
                } finally {
                    spillLock.unlock();
                }
            }

            try {
                // serialize key bytes into buffer,空间一般并为真正用尽，还可以往里写（但不能保证）
                int keystart = bufindex;
                keySerializer.serialize(key);  // 先把key串行化并写入bb
                if (bufindex < keystart) {
                    // wrapped the key; must make contiguous
                    bb.shiftBufferedKey();
                    keystart = 0;
                }
                // serialize value bytes into buffer
                final int valstart = bufindex;
                valSerializer.serialize(value); // 再把Value串行话并写入报表
                // It's possible for records to have zero length, i.e. the serializer
                // will perform no writes. To ensure that the boundary conditions are
                // checked and that the kvindex invariant is maintained, perform a
                // zero-length write into the buffer. The logic monitoring this could be
                // moved into collect, but this is cleaner and inexpensive. For now, it
                // is acceptable.
                bb.write(b0, 0, 0); // 注意写出的数据长度为0

                // the record must be marked after the preceding write, as the metadata
                // for this record are not yet written
                int valend = bb.markRecord();

                mapOutputRecordCounter.increment(1); // 修改统计信息，增加了一个记录
                mapOutputByteCounter.increment(
                        distanceTo(keystart, valend, bufvoid)); // 增加的字节数，填写元数据

                // write accounting info
                kvmeta.put(kvindex + PARTITION, partition);
                kvmeta.put(kvindex + KEYSTART, keystart);
                kvmeta.put(kvindex + VALSTART, valstart);
                kvmeta.put(kvindex + VALLEN, distanceTo(valstart, valend));
                // advance kvindex
                kvindex = (kvindex - NMETA + kvmeta.capacity()) % kvmeta.capacity(); // 调整kvindex
            } catch (MapBufferTooSmallException e) { // 发生因缓冲区大小所致的异常
                LOG.info("Record too large for in-memory buffer: " + e.getMessage());
                spillSingleRecord(key, value, partition); // KV对太大，缓冲区太小，直接Spill出去
                mapOutputRecordCounter.increment(1);
                return;
            }
        }

        private TaskAttemptID getTaskID() {
            return mapTask.getTaskID();
        }

        /**
         * Set the point from which meta and serialization data expand. The meta
         * indices are aligned with the buffer, so metadata never spans the ends of
         * the circular buffer.
         */
        private void setEquator(int pos) {
            equator = pos;
            // set index prior to first entry, aligned at meta boundary
            final int aligned = pos - (pos % METASIZE);
            // Cast one of the operands to long to avoid integer overflow
            kvindex = (int)
                    (((long) aligned - METASIZE + kvbuffer.length) % kvbuffer.length) / 4;
            LOG.info("(EQUATOR) " + pos + " kvi " + kvindex +
                    "(" + (kvindex * 4) + ")");
        }

        /**
         * The spill is complete, so set the buffer and meta indices to be equal to
         * the new equator to free space for continuing collection. Note that when
         * kvindex == kvend == kvstart, the buffer is empty.
         */
        private void resetSpill() {
            final int e = equator;
            bufstart = bufend = e; // 表现kv对数据块为空。
            final int aligned = e - (e % METASIZE);
            // set start/end to point to first meta record
            // Cast one of the operands to long to avoid integer overflow
            kvstart = kvend = (int)
                    (((long) aligned - METASIZE + kvbuffer.length) % kvbuffer.length) / 4; // 表示元数据块为空
            LOG.info("(RESET) equator " + e + " kv " + kvstart + "(" +
                    (kvstart * 4) + ")" + " kvi " + kvindex + "(" + (kvindex * 4) + ")");
        }

        /**
         * Compute the distance in bytes between two indices in the serialization
         * buffer.
         * @see #distanceTo(int, int, int)
         */
        final int distanceTo(final int i, final int j) {
            return distanceTo(i, j, kvbuffer.length);
        }

        /**
         * Compute the distance between two indices in the circular buffer given the
         * max distance.
         */
        int distanceTo(final int i, final int j, final int mod) {
            return i <= j
                    ? j - i
                    : mod - i + j;
        }

        /**
         * For the given meta position, return the offset into the int-sized
         * kvmeta buffer.
         */
        int offsetFor(int metapos) {
            return metapos * NMETA;
        }

        /**
         * Compare logical range, st i, j MOD offset capacity.
         * Compare by partition, then by key.
         * @see IndexedSortable#compare
         */
        @Override
        public int compare(final int mi, final int mj) {
            final int kvi = offsetFor(mi % maxRec);
            final int kvj = offsetFor(mj % maxRec);
            final int kvip = kvmeta.get(kvi + PARTITION); // 先比较PARTITION字段的内容
            final int kvjp = kvmeta.get(kvj + PARTITION);
            // sort by partition
            if (kvip != kvjp) {
                return kvip - kvjp; // 如果两PARTITION不同，那就已经比出了先后
            }
            // sort by key 如果PARTITION相同就再比两个Key的内容
            return comparator.compare(kvbuffer,
                    kvmeta.get(kvi + KEYSTART), // KEYSTART字段指向Key的起点
                    kvmeta.get(kvi + VALSTART) - kvmeta.get(kvi + KEYSTART), // Key的长度
                    kvbuffer, // 缓冲区的起点
                    kvmeta.get(kvj + KEYSTART),
                    kvmeta.get(kvj + VALSTART) - kvmeta.get(kvj + KEYSTART));
        }

        final byte META_BUFFER_TMP[] = new byte[METASIZE];

        /**
         * Swap metadata for items i, j
         * @see IndexedSortable#swap
         */
        @Override
        public void swap(final int mi, final int mj) {
            int iOff = (mi % maxRec) * METASIZE; // 元数据1的起点
            int jOff = (mj % maxRec) * METASIZE; // 元数据2的起点
            System.arraycopy(kvbuffer, iOff, META_BUFFER_TMP, 0, METASIZE);
            System.arraycopy(kvbuffer, jOff, kvbuffer, iOff, METASIZE);
            System.arraycopy(META_BUFFER_TMP, 0, kvbuffer, jOff, METASIZE);
        }

        /**
         * Inner class managing the spill of serialized records to disk.
         */
        protected class BlockingBuffer extends DataOutputStream {

            public BlockingBuffer() {
                super(new Buffer()); // 其核心是Buffer和kvbuffer
            }

            /**
             * Mark end of record. Note that this is required if the buffer is to
             * cut the spill in the proper place.
             */
            public int markRecord() {
                bufmark = bufindex;
                return bufindex;
            }

            /**
             * Set position from last mark to end of writable buffer, then rewrite
             * the data between last mark and kvindex.
             * This handles a special case where the key wraps around the buffer.
             * If the key is to be passed to a RawComparator, then it must be
             * contiguous in the buffer. This recopies the data in the buffer back
             * into itself, but starting at the beginning of the buffer. Note that
             * this method should <b>only</b> be called immediately after detecting
             * this condition. To call it at any other time is undefined and would
             * likely result in data loss or corruption.
             * @see #markRecord()
             */
            protected void shiftBufferedKey() throws IOException {
                // spillLock unnecessary; both kvend and kvindex are current
                int headbytelen = bufvoid - bufmark;
                bufvoid = bufmark;
                final int kvbidx = 4 * kvindex;
                final int kvbend = 4 * kvend;
                final int avail =
                        Math.min(distanceTo(0, kvbidx), distanceTo(0, kvbend));
                if (bufindex + headbytelen < avail) {
                    System.arraycopy(kvbuffer, 0, kvbuffer, headbytelen, bufindex);
                    System.arraycopy(kvbuffer, bufvoid, kvbuffer, 0, headbytelen);
                    bufindex += headbytelen;
                    bufferRemaining -= kvbuffer.length - bufvoid;
                } else {
                    byte[] keytmp = new byte[bufindex];
                    System.arraycopy(kvbuffer, 0, keytmp, 0, bufindex);
                    bufindex = 0;
                    out.write(kvbuffer, bufmark, headbytelen);
                    out.write(keytmp);
                }
            }
        }

        public class Buffer extends OutputStream { // 提供BlockingBuffer的writer()和flush()等操作
            private final byte[] scratch = new byte[1];

            @Override
            public void write(int v)
                    throws IOException {
                scratch[0] = (byte) v;
                write(scratch, 0, 1);
            }

            /**
             * Attempt to write a sequence of bytes to the collection buffer.
             * This method will block if the spill thread is running and it
             * cannot write.
             * @throws MapBufferTooSmallException if record is too large to
             *    deserialize into the collection buffer.
             */
            @Override
            public void write(byte b[], int off, int len)
                    throws IOException {
                // must always verify the invariant that at least METASIZE bytes are
                // available beyond kvindex, even when len == 0
                bufferRemaining -= len;
                if (bufferRemaining <= 0) {
                    // writing these bytes could exhaust available buffer space or fill
                    // the buffer to soft limit. check if spill or blocking are necessary
                    boolean blockwrite = false;
                    spillLock.lock();
                    try {
                        do {
                            checkSpillException();

                            final int kvbidx = 4 * kvindex;
                            final int kvbend = 4 * kvend;
                            // ser distance to key index
                            final int distkvi = distanceTo(bufindex, kvbidx);
                            // ser distance to spill end index
                            final int distkve = distanceTo(bufindex, kvbend);

                            // if kvindex is closer than kvend, then a spill is neither in
                            // progress nor complete and reset since the lock was held. The
                            // write should block only if there is insufficient space to
                            // complete the current write, write the metadata for this record,
                            // and write the metadata for the next record. If kvend is closer,
                            // then the write should block if there is too little space for
                            // either the metadata or the current write. Note that collect
                            // ensures its metadata requirement with a zero-length write
                            blockwrite = distkvi <= distkve
                                    ? distkvi <= len + 2 * METASIZE
                                    : distkve <= len || distanceTo(bufend, kvbidx) < 2 * METASIZE;

                            if (!spillInProgress) {
                                if (blockwrite) {
                                    if ((kvbend + METASIZE) % kvbuffer.length !=
                                            equator - (equator % METASIZE)) {
                                        // spill finished, reclaim space
                                        // need to use meta exclusively; zero-len rec & 100% spill
                                        // pcnt would fail
                                        resetSpill(); // resetSpill doesn't move bufindex, kvindex
                                        bufferRemaining = Math.min(
                                                distkvi - 2 * METASIZE,
                                                softLimit - distanceTo(kvbidx, bufindex)) - len;
                                        continue;
                                    }
                                    // we have records we can spill; only spill if blocked
                                    if (kvindex != kvend) {
                                        startSpill();
                                        // Blocked on this write, waiting for the spill just
                                        // initiated to finish. Instead of repositioning the marker
                                        // and copying the partial record, we set the record start
                                        // to be the new equator
                                        setEquator(bufmark);
                                    } else {
                                        // We have no buffered records, and this record is too large
                                        // to write into kvbuffer. We must spill it directly from
                                        // collect
                                        final int size = distanceTo(bufstart, bufindex) + len;
                                        setEquator(0);
                                        bufstart = bufend = bufindex = equator;
                                        kvstart = kvend = kvindex;
                                        bufvoid = kvbuffer.length;
                                        throw new MapBufferTooSmallException(size + " bytes");
                                    }
                                }
                            }

                            if (blockwrite) {
                                // wait for spill
                                try {
                                    while (spillInProgress) {
                                        reporter.progress();
                                        spillDone.await();
                                    }
                                } catch (InterruptedException e) {
                                    throw new IOException(
                                            "Buffer interrupted while waiting for the writer", e);
                                }
                            }
                        } while (blockwrite);
                    } finally {
                        spillLock.unlock();
                    }
                }
                // here, we know that we have sufficient space to write
                if (bufindex + len > bufvoid) {
                    final int gaplen = bufvoid - bufindex;
                    System.arraycopy(b, off, kvbuffer, bufindex, gaplen);
                    len -= gaplen;
                    off += gaplen;
                    bufindex = 0;
                }
                System.arraycopy(b, off, kvbuffer, bufindex, len);
                bufindex += len;
            }
        }

        public void flush() throws IOException, ClassNotFoundException,
                InterruptedException {
            LOG.info("Starting flush of map output");
            if (kvbuffer == null) {
                LOG.info("kvbuffer is null. Skipping flush.");
                return;
            }
            spillLock.lock();
            try {
                while (spillInProgress) { // 若有Spill正在进行，则等待其结束
                    reporter.progress();
                    spillDone.await();
                }
                checkSpillException();

                final int kvbend = 4 * kvend; // kvend是元数据块的终点，元数据块时向下伸展的，kvend是以整数计的数组下标，kvbend是以字节的数组下标
                if ((kvbend + METASIZE) % kvbuffer.length !=
                        equator - (equator % METASIZE)) { // 说明缓冲区原来是有数据的，现在Spill已经完成，需要释放空间
                    // spill finished  一次Spill刚完成，需要调整一些参数以释放缓冲区空间
                    resetSpill();
                }
                if (kvindex != kvend) { // 缓冲区非空，需要再Spill一次
                    kvend = (kvindex + NMETA) % kvmeta.capacity();
                    bufend = bufmark;
                    LOG.info("Spilling map output");
                    LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark +
                            "; bufvoid = " + bufvoid);
                    LOG.info("kvstart = " + kvstart + "(" + (kvstart * 4) +
                            "); kvend = " + kvend + "(" + (kvend * 4) +
                            "); length = " + (distanceTo(kvend, kvstart,
                            kvmeta.capacity()) + 1) + "/" + maxRec);
                    sortAndSpill();
                }
            } catch (InterruptedException e) {
                throw new IOException("Interrupted while waiting for the writer", e);
            } finally {
                spillLock.unlock();
            }
            assert !spillLock.isHeldByCurrentThread();
            // shut down spill thread and wait for it to exit. Since the preceding
            // ensures that it is finished with its work (and sortAndSpill did not
            // throw), we elect to use an interrupt instead of setting a flag.
            // Spilling simultaneously from this thread while the spill thread
            // finishes its work might be both a useful way to extend this and also
            // sufficient motivation for the latter approach.
            try {
                spillThread.interrupt(); // 使Spill线程不再运行
                spillThread.join(); // 结束Spill线程
            } catch (InterruptedException e) {
                throw new IOException("Spill failed", e);
            }
            // release sort buffer before the merge
            kvbuffer = null;
            mergeParts(); // 合并Spill文件
            Path outputPath = mapOutputFile.getOutputFile();
            fileOutputByteCounter.increment(rfs.getFileStatus(outputPath).getLen());
            // If necessary, make outputs permissive enough for shuffling.
            if (!SHUFFLE_OUTPUT_PERM.equals(
                    SHUFFLE_OUTPUT_PERM.applyUMask(FsPermission.getUMask(job)))) {
                Path indexPath = mapOutputFile.getOutputIndexFile();
                rfs.setPermission(outputPath, SHUFFLE_OUTPUT_PERM);
                rfs.setPermission(indexPath, SHUFFLE_OUTPUT_PERM);
            }
        }

        public void close() {
        }

        protected class SpillThread extends Thread {

            @Override
            public void run() {
                spillLock.lock();
                spillThreadRunning = true;
                try {
                    while (true) {
                        spillDone.signal();
                        while (!spillInProgress) {
                            spillReady.await(); // 等待需要Spill的通知
                        }
                        try {
                            spillLock.unlock();
                            sortAndSpill(); // 通过MapTask.sortAndSpill()将缓冲区的内容排序后希尔Spill文件
                        } catch (Throwable t) {
                            sortSpillException = t;
                        } finally {
                            spillLock.lock();
                            if (bufend < bufstart) {
                                bufvoid = kvbuffer.length;
                            }
                            kvstart = kvend;
                            bufstart = bufend;
                            spillInProgress = false;
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    spillLock.unlock();
                    spillThreadRunning = false;
                }
            }
        }

        private void checkSpillException() throws IOException {
            final Throwable lspillException = sortSpillException;
            if (lspillException != null) {
                if (lspillException instanceof Error) {
                    final String logMsg = "Task " + getTaskID() + " failed : " +
                            StringUtils.stringifyException(lspillException);
                    mapTask.reportFatalError(getTaskID(), lspillException, logMsg,
                            false);
                }
                throw new IOException("Spill failed", lspillException);
            }
        }

        private void startSpill() {
            assert !spillInProgress;
            kvend = (kvindex + NMETA) % kvmeta.capacity();
            bufend = bufmark;
            spillInProgress = true;
            LOG.info("Spilling map output");
            LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark +
                    "; bufvoid = " + bufvoid);
            LOG.info("kvstart = " + kvstart + "(" + (kvstart * 4) +
                    "); kvend = " + kvend + "(" + (kvend * 4) +
                    "); length = " + (distanceTo(kvend, kvstart,
                    kvmeta.capacity()) + 1) + "/" + maxRec);
            spillReady.signal(); // 通知SpillThread，开始Spill
        }

        private void sortAndSpill() throws IOException, ClassNotFoundException,
                InterruptedException {
            //approximate the length of the output file to be the length of the
            //buffer + header lengths for the partitions
            final long size = distanceTo(bufstart, bufend, bufvoid) +
                    partitions * APPROX_HEADER_LENGTH; // 环形缓冲区中的数据长度，加上每个Partition一个HEADER的长度
            FSDataOutputStream out = null;
            FSDataOutputStream partitionOut = null;
            try {
                // create spill file
                final SpillRecord spillRec = new SpillRecord(partitions);
                final Path filename =
                        mapOutputFile.getSpillFileForWrite(numSpills, size);
                out = rfs.create(filename); // 创建Spill文件

                final int mstart = kvend / NMETA; // num meta ints，计算共有几项元数据，以最后一项为起点
                final int mend = 1 + // kvend is a valid record ，计算第一项元数据的下标，把整个缓冲区看作一个元数据数组
                        (kvstart >= kvend
                                ? kvstart
                                : kvmeta.capacity() + kvstart) / NMETA;
                sorter.sort(MapOutputBuffer.this, mstart, mend, reporter); // 对元数据项排序，QuickSort.sort()
                int spindex = mstart; // 从最后一项（缓冲区属于元数据块的数组下标最小处）开始
                final IndexRecord rec = new IndexRecord(); // 创建索引记录
                final InMemValBytes value = new InMemValBytes();
                for (int i = 0; i < partitions; ++i) { // 对于每一个Partition
                    IFile.Writer<K, V> writer = null;
                    try {
                        long segmentStart = out.getPos(); // 获得Spill文件上的当前位置
                        partitionOut = CryptoUtils.wrapIfNecessary(job, out, false);
                        writer = new Writer<K, V>(job, partitionOut, keyClass, valClass, codec,
                                spilledRecordsCounter); // 创建以Spill文件为目标的加密或不加密Writer
                        if (combinerRunner == null) { //spill directly，不经过combinerRunner
                            // spill directly
                            DataInputBuffer key = new DataInputBuffer();
                            while (spindex < mend &&
                                    kvmeta.get(offsetFor(spindex % maxRec) + PARTITION) == i) { // 扫描全部元数据，如果某项元数据中的PARTITION字段表明该记录属于PARTITION_i
                                final int kvoff = offsetFor(spindex % maxRec); // maxRec = kvmeta.capacity()/NMETA
                                int keystart = kvmeta.get(kvoff + KEYSTART); // key所在的位置
                                int valstart = kvmeta.get(kvoff + VALSTART); // val所在的位置
                                key.reset(kvbuffer, keystart, valstart - keystart); // 将串行化了的K值转移到DataInputBuffer中
                                getVBytesForOffset(kvoff, value); // 将串行化了的K值转移到InMemValBytes中
                                writer.append(key, value); // 写入Spill文件
                                ++spindex; // 往前推进
                            }
                        } else { // 如果需要经过combinerRunner
                            int spstart = spindex;
                            while (spindex < mend &&
                                    kvmeta.get(offsetFor(spindex % maxRec)
                                            + PARTITION) == i) {
                                ++spindex; // 直到碰上属于下一个partition的数据
                            }
                            // Note: we would like to avoid the combiner if we've fewer
                            // than some threshold of records for a partition
                            if (spstart != spindex) { // 如果有数据
                                combineCollector.setWriter(writer); // =CombineOutputCollector.setWriter(writer)
                                RawKeyValueIterator kvIter =
                                        new MRResultIterator(spstart, spindex);
                                combinerRunner.combine(kvIter, combineCollector); // 在写出过程中加上combine()环节，combineCollector为CombineOutputCollector
                            }
                        } // 经过combinerRunner

                        // close the writer
                        writer.close(); // 关闭writer，当前这个partition已完成spill
                        if (partitionOut != out) {
                            partitionOut.close();
                            partitionOut = null;
                        }

                        // record offsets
                        rec.startOffset = segmentStart; // 记录在IndexRecord中，一个Partition一个索引记录
                        rec.rawLength = writer.getRawLength() + CryptoUtils.cryptoPadding(job);
                        rec.partLength = writer.getCompressedLength() + CryptoUtils.cryptoPadding(job);
                        spillRec.putIndex(rec, i); // 将partition i的IndexRecord放入SpillRecord

                        writer = null;
                    } finally {
                        if (null != writer) writer.close();
                    }
                }

                if (totalIndexCacheMemory >= indexCacheMemoryLimit) {
                    // create spill index file 索引块太大，需要创建并写入Spill索引文件
                    Path indexFilename =
                            mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
                                    * MAP_OUTPUT_INDEX_RECORD_LENGTH);
                    spillRec.writeToFile(indexFilename, job); // 将SpillRecord写入indexFile
                } else { // 索引块不大，可以留在内存中
                    indexCacheList.add(spillRec);
                    totalIndexCacheMemory +=
                            spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
                }
                LOG.info("Finished spill " + numSpills);
                ++numSpills; // 增加了一次Spill
            } finally {
                if (out != null) out.close();
                if (partitionOut != null) {
                    partitionOut.close();
                }
            }
        }

        /**
         * Handles the degenerate case where serialization fails to fit in
         * the in-memory buffer, so we must spill the record from collect
         * directly to a spill file. Consider this "losing".
         */
        private void spillSingleRecord(final K key, final V value,
                                       int partition) throws IOException {
            long size = kvbuffer.length + partitions * APPROX_HEADER_LENGTH;
            FSDataOutputStream out = null;
            FSDataOutputStream partitionOut = null;
            try {
                // create spill file
                final SpillRecord spillRec = new SpillRecord(partitions);
                final Path filename =
                        mapOutputFile.getSpillFileForWrite(numSpills, size);
                out = rfs.create(filename);

                // we don't run the combiner for a single record
                IndexRecord rec = new IndexRecord();
                for (int i = 0; i < partitions; ++i) {
                    IFile.Writer<K, V> writer = null;
                    try {
                        long segmentStart = out.getPos();
                        // Create a new codec, don't care!
                        partitionOut = CryptoUtils.wrapIfNecessary(job, out, false);
                        writer = new IFile.Writer<K, V>(job, partitionOut, keyClass, valClass, codec,
                                spilledRecordsCounter);

                        if (i == partition) {
                            final long recordStart = out.getPos();
                            writer.append(key, value);
                            // Note that our map byte count will not be accurate with
                            // compression
                            mapOutputByteCounter.increment(out.getPos() - recordStart);
                        }
                        writer.close();
                        if (partitionOut != out) {
                            partitionOut.close();
                            partitionOut = null;
                        }

                        // record offsets
                        rec.startOffset = segmentStart;
                        rec.rawLength = writer.getRawLength() + CryptoUtils.cryptoPadding(job);
                        rec.partLength = writer.getCompressedLength() + CryptoUtils.cryptoPadding(job);
                        spillRec.putIndex(rec, i);

                        writer = null;
                    } catch (IOException e) {
                        if (null != writer) writer.close();
                        throw e;
                    }
                }
                if (totalIndexCacheMemory >= indexCacheMemoryLimit) {
                    // create spill index file
                    Path indexFilename =
                            mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
                                    * MAP_OUTPUT_INDEX_RECORD_LENGTH);
                    spillRec.writeToFile(indexFilename, job);
                } else {
                    indexCacheList.add(spillRec);
                    totalIndexCacheMemory +=
                            spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
                }
                ++numSpills;
            } finally {
                if (out != null) out.close();
                if (partitionOut != null) {
                    partitionOut.close();
                }
            }
        }

        /**
         * Given an offset, populate vbytes with the associated set of
         * deserialized value bytes. Should only be called during a spill.
         */
        private void getVBytesForOffset(int kvoff, InMemValBytes vbytes) {
            // get the keystart for the next serialized value to be the end
            // of this value. If this is the last value in the buffer, use bufend
            final int vallen = kvmeta.get(kvoff + VALLEN);
            assert vallen >= 0;
            vbytes.reset(kvbuffer, kvmeta.get(kvoff + VALSTART), vallen);
        }

        /**
         * Inner class wrapping valuebytes, used for appendRaw.
         */
        protected class InMemValBytes extends DataInputBuffer {
            private byte[] buffer;
            private int start;
            private int length;

            public void reset(byte[] buffer, int start, int length) {
                this.buffer = buffer;
                this.start = start;
                this.length = length;

                if (start + length > bufvoid) {
                    this.buffer = new byte[this.length];
                    final int taillen = bufvoid - start;
                    System.arraycopy(buffer, start, this.buffer, 0, taillen);
                    System.arraycopy(buffer, 0, this.buffer, taillen, length - taillen);
                    this.start = 0;
                }

                super.reset(this.buffer, this.start, this.length);
            }
        }

        protected class MRResultIterator implements RawKeyValueIterator {
            private final DataInputBuffer keybuf = new DataInputBuffer();
            private final InMemValBytes vbytes = new InMemValBytes();
            private final int end;
            private int current;

            public MRResultIterator(int start, int end) {
                this.end = end;
                current = start - 1;
            }

            public boolean next() throws IOException {
                return ++current < end;
            }

            public DataInputBuffer getKey() throws IOException {
                final int kvoff = offsetFor(current % maxRec);
                keybuf.reset(kvbuffer, kvmeta.get(kvoff + KEYSTART),
                        kvmeta.get(kvoff + VALSTART) - kvmeta.get(kvoff + KEYSTART));
                return keybuf;
            }

            public DataInputBuffer getValue() throws IOException {
                getVBytesForOffset(offsetFor(current % maxRec), vbytes);
                return vbytes;
            }

            public Progress getProgress() {
                return null;
            }

            public void close() {
            }
        }

        private void mergeParts() throws IOException, InterruptedException,
                ClassNotFoundException {
            // get the approximate size of the final output/index files
            long finalOutFileSize = 0;
            long finalIndexFileSize = 0;
            final Path[] filename = new Path[numSpills]; // 每次Spill都有个文件，所以数组大小为numSpills
            final TaskAttemptID mapId = getTaskID();

            for (int i = 0; i < numSpills; i++) { // 统计所有这些文件合并之后的大小
                filename[i] = mapOutputFile.getSpillFile(i); // 获取文件名
                finalOutFileSize += rfs.getFileStatus(filename[i]).getLen(); // 获取文件大小
            }
            if (numSpills == 1) { //the spill is the final output 如果只有一次spill，那么就简单了
                sameVolRename(filename[0],
                        mapOutputFile.getOutputFileForWriteInVolume(filename[0])); // 环境文件名，在原名上加后缀file.out
                if (indexCacheList.size() == 0) { // 索引块缓存 indexCacheList已空
                    sameVolRename(mapOutputFile.getSpillIndexFile(0),
                            mapOutputFile.getOutputIndexFileForWriteInVolume(filename[0])); // SpillIndexFile改名
                } else { // 索引块缓存indexCacheList中还有所以记录，要写到索引文件
                    indexCacheList.get(0).writeToFile(
                            mapOutputFile.getOutputIndexFileForWriteInVolume(filename[0]), job);
                }
                sortPhase.complete();
                return; // 如果只有一个spill，这就已完成"合并"
            }

            // read in paged indices
            for (int i = indexCacheList.size(); i < numSpills; ++i) { // Spill文件不止一个，需要合并
                Path indexFileName = mapOutputFile.getSpillIndexFile(i);
                indexCacheList.add(new SpillRecord(indexFileName, job)); // 先将所有SpillIndexFile收集在一起
            }

            //make correction in the length to include the sequence file header
            //lengths for each partition
            finalOutFileSize += partitions * APPROX_HEADER_LENGTH; // 每个partition都有个HEADER
            finalIndexFileSize = partitions * MAP_OUTPUT_INDEX_RECORD_LENGTH; // IndexFile，每个partition一个记录
            Path finalOutputFile =
                    mapOutputFile.getOutputFileForWrite(finalOutFileSize);
            Path finalIndexFile =
                    mapOutputFile.getOutputIndexFileForWrite(finalIndexFileSize);

            //The output stream for the final single output file 创建（合并后的）最终输出文件
            FSDataOutputStream finalOut = rfs.create(finalOutputFile, true, 4096);
            FSDataOutputStream finalPartitionOut = null;

            if (numSpills == 0) { // 要是压根就没有生成SpillFile，也要创建空文件
                //create dummy files
                IndexRecord rec = new IndexRecord(); // 创建索引记录
                SpillRecord sr = new SpillRecord(partitions); // 创建Spill记录
                try {
                    for (int i = 0; i < partitions; i++) {
                        long segmentStart = finalOut.getPos();
                        finalPartitionOut = CryptoUtils.wrapIfNecessary(job, finalOut,
                                false);
                        Writer<K, V> writer =
                                new Writer<K, V>(job, finalPartitionOut, keyClass, valClass, codec, null);
                        writer.close();
                        if (finalPartitionOut != finalOut) {
                            finalPartitionOut.close();
                            finalPartitionOut = null;
                        }
                        rec.startOffset = segmentStart;
                        rec.rawLength = writer.getRawLength() + CryptoUtils.cryptoPadding(job);
                        rec.partLength = writer.getCompressedLength() + CryptoUtils.cryptoPadding(job);
                        sr.putIndex(rec, i);
                    }
                    sr.writeToFile(finalIndexFile, job); // 将索引记录写入索引文件
                } finally {
                    finalOut.close();
                    if (finalPartitionOut != null) {
                        finalPartitionOut.close();
                    }
                }
                sortPhase.complete();
                return;
            }
            {
                sortPhase.addPhases(partitions); // Divide sort phase into sub-phases

                IndexRecord rec = new IndexRecord();
                final SpillRecord spillRec = new SpillRecord(partitions);
                for (int parts = 0; parts < partitions; parts++) { // 对于每一个Partition
                    //create the segments to be merged
                    List<Segment<K, V>> segmentList =
                            new ArrayList<Segment<K, V>>(numSpills);
                    for (int i = 0; i < numSpills; i++) { // 准备合并其所有Spill文件
                        IndexRecord indexRecord = indexCacheList.get(i).getIndex(parts);

                        Segment<K, V> s =
                                new Segment<K, V>(job, rfs, filename[i], indexRecord.startOffset,
                                        indexRecord.partLength, codec, true);
                        segmentList.add(i, s); // 把每个Spill文件中属于同一Partition的区段位置收集起来

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("MapId=" + mapId + " Reducer=" + parts +
                                    "Spill =" + i + "(" + indexRecord.startOffset + "," +
                                    indexRecord.rawLength + ", " + indexRecord.partLength + ")");
                        }
                    }

                    int mergeFactor = job.getInt(MRJobConfig.IO_SORT_FACTOR,
                            MRJobConfig.DEFAULT_IO_SORT_FACTOR);
                    // sort the segments only if there are intermediate merges
                    boolean sortSegments = segmentList.size() > mergeFactor;
                    //merge
                    @SuppressWarnings("unchecked")
                    RawKeyValueIterator kvIter = Merger.merge(job, rfs,
                            keyClass, valClass, codec,
                            segmentList, mergeFactor,
                            new Path(mapId.toString()),
                            job.getOutputKeyComparator(), reporter, sortSegments,
                            null, spilledRecordsCounter, sortPhase.phase(),
                            TaskType.MAP); // 合并同一Patition在所有Spill文件中的内容，也可能还需要sort，合并的结果是一个序列kvIter

                    //write merged output to disk
                    long segmentStart = finalOut.getPos();
                    finalPartitionOut = CryptoUtils.wrapIfNecessary(job, finalOut, false);
                    Writer<K, V> writer =
                            new Writer<K, V>(job, finalPartitionOut, keyClass, valClass, codec,
                                    spilledRecordsCounter);
                    if (combinerRunner == null || numSpills < minSpillsForCombine) { // 如果无需combine
                        Merger.writeFile(kvIter, writer, reporter, job); // 就将合并的结果直接写入问
                    } else {
                        combineCollector.setWriter(writer); // 如果需要combine，那就插入combine环节
                        combinerRunner.combine(kvIter, combineCollector); // 合并结果经combine后写入文件
                    }

                    //close
                    writer.close();
                    if (finalPartitionOut != finalOut) {
                        finalPartitionOut.close();
                        finalPartitionOut = null;
                    }

                    sortPhase.startNextPhase();

                    // record offsets
                    rec.startOffset = segmentStart; // 从当前段的起点开始
                    rec.rawLength = writer.getRawLength() + CryptoUtils.cryptoPadding(job); // 长度
                    rec.partLength = writer.getCompressedLength() + CryptoUtils.cryptoPadding(job);
                    spillRec.putIndex(rec, parts);
                }
                spillRec.writeToFile(finalIndexFile, job); // 将spillRec写入合并的IndexFile
                finalOut.close(); // 关闭最终输出文件
                if (finalPartitionOut != null) {
                    finalPartitionOut.close();
                }
                for (int i = 0; i < numSpills; i++) {
                    rfs.delete(filename[i], true); // 删掉所有的Spill文件
                }
            }
        }

        /**
         * Rename srcPath to dstPath on the same volume. This is the same
         * as RawLocalFileSystem's rename method, except that it will not
         * fall back to a copy, and it will create the target directory
         * if it doesn't exist.
         */
        private void sameVolRename(Path srcPath,
                                   Path dstPath) throws IOException {
            RawLocalFileSystem rfs = (RawLocalFileSystem) this.rfs;
            File src = rfs.pathToFile(srcPath);
            File dst = rfs.pathToFile(dstPath);
            if (!dst.getParentFile().exists()) {
                if (!dst.getParentFile().mkdirs()) {
                    throw new IOException("Unable to rename " + src + " to "
                            + dst + ": couldn't create parent directory");
                }
            }

            if (!src.renameTo(dst)) {
                throw new IOException("Unable to rename " + src + " to " + dst);
            }
        }
    } // MapOutputBuffer

    /**
     * Exception indicating that the allocated sort buffer is insufficient
     * to hold the current record.
     */
    @SuppressWarnings("serial")
    private static class MapBufferTooSmallException extends IOException {
        public MapBufferTooSmallException(String s) {
            super(s);
        }
    }

    private <INKEY, INVALUE, OUTKEY, OUTVALUE>
    void closeQuietly(RecordReader<INKEY, INVALUE> c) {
        if (c != null) {
            try {
                c.close();
            } catch (IOException ie) {
                // Ignore
                LOG.info("Ignoring exception during close for " + c, ie);
            }
        }
    }

    private <OUTKEY, OUTVALUE>
    void closeQuietly(MapOutputCollector<OUTKEY, OUTVALUE> c) {
        if (c != null) {
            try {
                c.close();
            } catch (Exception ie) {
                // Ignore
                LOG.info("Ignoring exception during close for " + c, ie);
            }
        }
    }

    private <INKEY, INVALUE, OUTKEY, OUTVALUE>
    void closeQuietly(
            org.apache.hadoop.mapreduce.RecordReader<INKEY, INVALUE> c) {
        if (c != null) {
            try {
                c.close();
            } catch (Exception ie) {
                // Ignore
                LOG.info("Ignoring exception during close for " + c, ie);
            }
        }
    }

    private <INKEY, INVALUE, OUTKEY, OUTVALUE>
    void closeQuietly(
            org.apache.hadoop.mapreduce.RecordWriter<OUTKEY, OUTVALUE> c,
            org.apache.hadoop.mapreduce.Mapper<INKEY, INVALUE, OUTKEY, OUTVALUE>.Context
                    mapperContext) {
        if (c != null) {
            try {
                c.close(mapperContext);
            } catch (Exception ie) {
                // Ignore
                LOG.info("Ignoring exception during close for " + c, ie);
            }
        }
    }
}
