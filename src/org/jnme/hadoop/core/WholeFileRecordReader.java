package org.jnme.hadoop.core;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

// vv WholeFileRecordReade
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private static final Log LOG = LogFactory.getLog(WholeFileRecordReader.class);
    private FileSplit fileSplit;
    private Configuration conf;
    private boolean processed = false;
    private Text key = null;
    private BytesWritable value = null;

    public WholeFileRecordReader(FileSplit fileSplit, Configuration conf)
            throws IOException {
        this.fileSplit = fileSplit;
        this.conf = conf;
    }

    public WholeFileRecordReader(FileSplit fileSplit, TaskAttemptContext task)
            throws IOException {
        this.fileSplit = fileSplit;
        this.conf = task.getConfiguration();
    }

    public Text createKey() {
        return new Text();
    }

    public BytesWritable createValue() {
        value = new BytesWritable();
        return value;
    }

    public long getPos() throws IOException {
        return processed ? fileSplit.getLength() : 0;
    }

    public float getProgress() throws IOException {
        return processed ? 1.0f : 0.0f;
    }

    /*
    public boolean next(NullWritable key, BytesWritable value) throws IOException {
    log.info(" File Recorder Processing file ");
    if (!processed) {
    byte[] contents = new byte[(int) fileSplit.getLength()];
    Path file = fileSplit.getPath();
    log.info("Processing file " + fileSplit.getPath().getName());
    FileSystem fs = file.getFileSystem(conf);
    FSDataInputStream in = null;
    try {
    in = fs.open(file);
    IOUtils.readFully(in, contents, 0, contents.length);
    value.set(contents, 0, contents.length);
    } finally {
    IOUtils.closeStream(in);
    }
    processed = true;
    return true;
    }
    return false;
    }
     */
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return this.key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        // TODO Auto-generated method stub
        return this.value;
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1)
            throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            byte[] contents = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = null;

            if (this.key == null) {
                this.key = new Text();
            }

            if (this.value == null) {
                this.value = new BytesWritable();
            }

            try {
                in = fs.open(file);
                IOUtils.readFully(in, contents, 0, contents.length);
                this.key.set(file.getName());
                this.value.set(contents, 0, contents.length);
            }catch(Exception e){
                LOG.error(e.getMessage());
            } finally {
                IOUtils.closeStream(in);
            }
            processed = true;
            return true;
        }
        return false;
    }
}
