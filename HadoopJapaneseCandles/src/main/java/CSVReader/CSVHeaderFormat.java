package CSVReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import java.io.IOException;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


import java.util.HashMap;
import java.util.Map;



public class CSVHeaderFormat extends FileInputFormat<Text, Text> {
    Text csv_header;
    String[] record = new String[4];
    Map<Integer, String> idx_cols = new HashMap<>();

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CSVReader(context.getConfiguration());
    }
    //genericSplit - the split that defines the range of records to read
    //context - the information about the task

    public class CSVReader extends KeyValueLineRecordReader{
        public CSVReader(Configuration conf) throws IOException {
            super(conf);
        }

        @Override
        public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException {
            super.initialize(genericSplit, context);
            super.nextKeyValue();
            csv_header = super.getCurrentKey();
            if (csv_header.toString().length() == 0) {
                throw new RuntimeException("Broken file: Cannot find CSV-header!");
            }
            String[] columns = csv_header.toString().split(",");
            int idx = 0;
            for (String col: columns) {
                idx_cols.put(idx, col);
                idx++;
            }
        }
        //return same as Value
        @Override
        public Text getCurrentKey() {
            return this.getCurrentValue();
        }

        //return correctly readed values
        @Override
        public Text getCurrentValue() {
            String[] cols_data = super.getCurrentKey().toString().split(",");
            // String[] cols = line.;
            int idx = 0;
            // rearrange line to SYNBOL MOMENT PRICE ID
            for (String data: cols_data) {
                String col_name = idx_cols.get(idx);
                switch (col_name) {
                    case "#SYMBOL":
                        record[0] = data;
                        break;
                    case "MOMENT":
                        record[1] = data;
                        break;
                    case "PRICE_DEAL":
                        record[2] = data;
                        break;
                    case "ID_DEAL":
                        record[3] = data;
                        break;
                }
                idx++;
            }
            String filteredLine = record[0] + "," + record[1] + "," + record[2] + "," + record[3];
            return new Text(filteredLine);
        }
    };
}