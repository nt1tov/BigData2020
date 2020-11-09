package org.apache.hadoop.examples;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class CSVInputFormat extends FileInputFormat<Text, Text> {

    @Override
    public RecordReader<Text, Text>
    createRecordReader(InputSplit split, TaskAttemptContext context) {
        Character separator = ',';
        return new CSVRecordReader(separator);
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return true;
    }

    public static class CSVRecordReader extends RecordReader<Text, Text> {
        private LineRecordReader reader;
        private Text key;
        private Text value;

        public CSVRecordReader(Character csvDelimiter) {
            this.reader = new LineRecordReader();
            key = new Text();
            value = new Text();
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            reader.initialize(split, context);
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (reader.nextKeyValue()) {
                loadCSV();
                return true;
            }
            else {
                value = null;
                return false;
            }
        }

        private void loadCSV() throws IOException {
            String line = reader.getCurrentValue().toString();
            if (line.contains("#SYMBOL")) {
                key.set(line);
            }
            else {
                value.set(line);
            }
        }

        private Text[] convert(String[] s) {
            Text t[] = new Text[s.length];
            for(int i=0; i < t.length; i++) {
                t[i] = new Text(s[i]);
            }
            return t;
        }

        @Override
        public Text getCurrentKey()      //<co id="ch02_comment_csv_inputformat11"/>
                throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Text getCurrentValue()    //<co id="ch02_comment_csv_inputformat12"/>
                throws IOException, InterruptedException {
            return value;
        }

        @Override
        public float getProgress()
                throws IOException, InterruptedException {
            return reader.getProgress();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}