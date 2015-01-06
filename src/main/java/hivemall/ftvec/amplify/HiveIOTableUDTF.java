/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2015
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.ftvec.amplify;

import hivemall.utils.hadoop.HiveUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.thirdparty.guava.common.collect.AbstractIterator;

import com.facebook.hiveio.common.HiveTableDesc;
import com.facebook.hiveio.common.HiveType;
import com.facebook.hiveio.input.HiveApiInputFormat;
import com.facebook.hiveio.input.HiveIOUtils;
import com.facebook.hiveio.input.HiveInputDescription;
import com.facebook.hiveio.record.HiveReadableRecord;

public final class HiveIOTableUDTF extends GenericUDTF {
    private static final Log logger = LogFactory.getLog(HiveIOTableUDTF.class);

    private int _amplify;

    private HiveInputDescription _inputDesc;
    private Configuration _jobConf;
    private int _numFields;

    @Override
    public void configure(MapredContext mapredContext) {
        this._jobConf = mapredContext.getJobConf();
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        if(argOIs.length != 2) {
            throw new UDFArgumentException("Expected two arguments {tableName:string, amplificationFactor: int}: "
                    + Arrays.toString(argOIs));
        }
        String tableName = HiveUtils.getConstString(argOIs[0]);
        this._amplify = HiveUtils.getConstInt(argOIs[1]);
        if(_amplify < 1) {
            throw new UDFArgumentException("amplification factor must be greater than 0: "
                    + _amplify);
        }

        HiveInputDescription inputDesc = new HiveInputDescription();
        configureTableDesc(inputDesc, tableName);
        this._inputDesc = inputDesc;

        if(_jobConf == null) {
            this._jobConf = com.facebook.hiveio.common.HiveUtils.newHiveConf();
        }

        final StructObjectInspector inputOI;
        try {
            inputOI = HiveIOUtils.getStructObjectInspector(inputDesc, _jobConf);
        } catch (Exception e) {
            throw new UDFArgumentException(e);
        }
        StructObjectInspector outputOI = HiveIOUtils.toJavaObjectOI(inputOI);
        this._numFields = outputOI.getAllStructFieldRefs().size();
        return outputOI;
    }

    @Override
    public void process(Object[] args) throws HiveException {}

    @Override
    public void close() throws HiveException {
        assert (_inputDesc != null);
        assert (_jobConf != null);

        final int columns = _numFields;
        final Object[] forwardObjs = new Object[columns];

        final Iterable<HiveReadableRecord> table;
        try {
            table = readTableShuffled(_inputDesc, _jobConf, _amplify);
        } catch (Exception e) {
            throw new HiveException(e);
        }
        Iterator<HiveReadableRecord> records = table.iterator();
        while(records.hasNext()) {
            HiveReadableRecord record = records.next();
            for(int i = 0; i < columns; i++) {
                HiveType type = record.columnType(i);
                Object obj = record.get(i, type);
                forwardObjs[i] = obj;
            }
            forward(forwardObjs);
        }

        this._inputDesc = null;
        this._jobConf = null;
    }

    private static void configureTableDesc(@Nonnull HiveInputDescription inputDesc, @Nullable String tableName)
            throws UDFArgumentException {
        if(tableName == null) {
            throw new UDFArgumentException("tableName was null");
        }
        HiveTableDesc tableDesc = inputDesc.getTableDesc();
        String[] tableNames = tableName.split("\\.");
        if(tableNames.length == 1) {
            tableDesc.setTableName(tableNames[0]);
        } else if(tableNames.length == 2) {
            tableDesc.setDatabaseName(tableNames[0]);
            tableDesc.setTableName(tableNames[1]);
        } else {
            throw new UDFArgumentException("Invalid tableName: " + tableName);
        }
    }

    private static Iterable<HiveReadableRecord> readTableShuffled(@Nonnull final HiveInputDescription inputDesc, @Nonnull final Configuration conf, @Nonnegative final int amplify)
            throws IOException, InterruptedException {
        String profileID = Long.toString(System.currentTimeMillis());

        HiveApiInputFormat.setProfileInputDesc(conf, inputDesc, profileID);

        final HiveApiInputFormat inputFormat = new HiveApiInputFormat();
        inputFormat.setMyProfileId(profileID);

        JobContext jobContext = new JobContext(conf, new JobID());
        List<InputSplit> splits = inputFormat.getSplits(jobContext);

        int size = splits.size() * amplify;
        final List<InputSplit> shuffled = new ArrayList<InputSplit>(size);
        for(int i = 0; i < amplify; i++) {
            shuffled.addAll(splits);
        }
        Collections.shuffle(shuffled);

        return new Iterable<HiveReadableRecord>() {
            @Override
            public Iterator<HiveReadableRecord> iterator() {
                return new RecordIterator(inputFormat, conf, shuffled.iterator());
            }
        };
    }

    /**
     * Iterator over records
     */
    private static class RecordIterator extends AbstractIterator<HiveReadableRecord> {
        /** input format */
        private final HiveApiInputFormat inputFormat;
        /** input splits */
        private final Iterator<InputSplit> splits;
        /** context */
        private final TaskAttemptContext taskContext;
        /** record reader */
        private RecordReaderWrapper<HiveReadableRecord> recordReader;

        /**
         * Constructor
         *
         * @param inputFormat Hive table InputFormat
         * @param conf Configuration
         * @param splits input splits
         */
        public RecordIterator(HiveApiInputFormat inputFormat, Configuration conf, Iterator<InputSplit> splits) {
            this.inputFormat = inputFormat;
            this.splits = splits;
            this.taskContext = new TaskAttemptContext(conf, new TaskAttemptID());
        }

        @SuppressWarnings("rawtypes")
        @Override
        protected HiveReadableRecord computeNext() {
            while(recordReader == null || !recordReader.hasNext()) {
                RecordReader<WritableComparable, HiveReadableRecord> reader = null;
                while(splits.hasNext() && reader == null) {
                    InputSplit split = splits.next();
                    try {
                        reader = inputFormat.createRecordReader(split, taskContext);
                        reader.initialize(split, taskContext);
                    } catch (Exception e) {
                        logger.info("Couldn't create reader from split: " + split, e);
                    }
                }
                if(reader == null) {
                    return endOfData();
                } else {
                    recordReader = new RecordReaderWrapper<HiveReadableRecord>(reader);
                }
            }

            return recordReader.next();
        }
    }

    @SuppressWarnings("rawtypes")
    private static class RecordReaderWrapper<T> extends AbstractIterator<T> {
        /** Wrapped {@link RecordReader} */
        private final RecordReader<WritableComparable, T> recordReader;

        /**
         * Constructor
         *
         * @param recordReader {@link RecordReader} to wrap
         */
        public RecordReaderWrapper(RecordReader<WritableComparable, T> recordReader) {
            this.recordReader = recordReader;
        }

        @Override
        protected T computeNext() {
            try {
                if(!recordReader.nextKeyValue()) {
                    endOfData();
                    return null;
                }
                return recordReader.getCurrentValue();
            } catch (IOException e) {
                throw new IllegalStateException("computeNext: IOException occurred");
            } catch (InterruptedException e) {
                throw new IllegalStateException("computeNext: InterruptedException occurred");
            }
        }
    }

}
