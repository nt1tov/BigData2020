import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import CSVReader.CSVHeaderFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class candle {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{

        private  Text endkey  = new Text();
        private Text endval  = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         //   StringTokenizer itr = new StringTokenizer(value.toString());
            Configuration conf = context.getConfiguration();
            // separated with CSVHeaderFormat package
            // SYNBOL MOMENT PRICE ID
            String[] record_split = value.toString().split(",");
            String SYMBOL = record_split[0];
            String MOMENT = record_split[1];
            String PRICE = record_split[2];
            String ID = record_split[3];
            String sec_ptrn = conf.get("candle.securities");
            if(sec_ptrn != null ){
                if(sec_ptrn.startsWith("\"")){
                    sec_ptrn = sec_ptrn.substring(1, sec_ptrn.length());
                }
                if(sec_ptrn.endsWith("\"")){
                    sec_ptrn = sec_ptrn.substring(0, sec_ptrn.length()-1);
                }
            }

            if(!SYMBOL.matches(sec_ptrn)){
                return;
            }
            String cur_time = MOMENT.substring(8,12);
            String time_from = conf.get("candle.time.from");
            String time_to = conf.get("candle.time.to");
            if (cur_time.compareTo(time_to) < 0 || cur_time.compareTo(time_from) > 0){
                return;
            }

        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        conf.setIfUnset("candle.width", "300000");
        conf.setIfUnset("candle.securities", ".*");
        conf.setIfUnset("candle.date.from", "19000101");
        conf.setIfUnset("candle.date.to", "20200101");
        conf.setIfUnset("candle.time.from", "1800");
        conf.setIfUnset("candle.time.to", "1800");
        conf.setIfUnset("candle.num.reducers", "1");


        Job job = new Job(conf, "candles ");
   //     job.setNumReduceTasks(conf.getInt("candle.num.reducers", 1));
        job.setJarByClass(candle.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        // job.setCombinerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(CSVHeaderFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}