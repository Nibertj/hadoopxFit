/**
 * 
 */
package org.jnme.hadoop.core;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * @author nibertj
 *
 */
public class TextRecursiveFileInputFormat extends RecursiveFileInputFormat<Text, BytesWritable> {

    private static final Log log = LogFactory.getLog(TextRecursiveFileInputFormat.class);
    
    /*@Override
    protected boolean isSplitable(JobContext context, Path file) { 
            return false;
    }*/
    
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split,
            TaskAttemptContext job) throws IOException, InterruptedException {
        log.info("Inside the record reader");

        return new WholeFileRecordReader((FileSplit) split, job);
    }
}
