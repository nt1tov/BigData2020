import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.CSVInputFormat;
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

    public static class TokenizerMapper
            extends Mapper<IntWritable, Text, Text, Text>{

        private  Text endkey  = new Text();
        private Text endval  = new Text();

        public void map(IntWritable key, Text record, Context context) throws IOException, InterruptedException {
         //   StringTokenizer itr = new StringTokenizer(value.toString());
            Configuration conf = context.getConfiguration();

            if(key.equals(0)){
                return;
            }

            String[] tokens = record.toString().split(",");
            try
            {
                int action = Integer.parseInt(actionStr);
                int campaign = Integer.parseInt(campaignStr);

                IntWritable mapOutKey = new IntWritable(campaign);
                IntWritable mapOutValue = new IntWritable(action);
                context.write(mapOutKey, mapOutValue);
            } catch (Exception e)
            {
                System.out.println("*** exception:");
                e.printStackTrace();
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
        job.setInputFormatClass(CSVInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}