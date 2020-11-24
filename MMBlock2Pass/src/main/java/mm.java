import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import CSVReader.CSVHeaderFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.logging.LogFactory;

public class mm {
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
        private  Text endkey  = new Text();
        private Text endval  = new Text();
        public static final Log log = LogFactory.getLog(TokenizerMapper.class); // поле класса
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         //   StringTokenizer itr = new StringTokenizer(value.toString());
            Configuration conf = context.getConfiguration();
            // separated with CSVHeaderFormat package
            // SYMBOL MOMENT PRICE ID
            String[] record_split = value.toString().split(",");
            String SYMBOL = record_split[0];
            String MOMENT = record_split[1];
            String PRICE = record_split[2];
            String ID = record_split[3];
          //  log.info(value.toString());
            String sec_ptrn = conf.get("candle.securities");
            assert sec_ptrn != null;
            // cut
            if(sec_ptrn.startsWith("\"")){
                sec_ptrn = sec_ptrn.substring(1, sec_ptrn.length());
            }
            if(sec_ptrn.endsWith("\"")){
                sec_ptrn = sec_ptrn.substring(0, sec_ptrn.length()-1);
            }

            if(!SYMBOL.matches(sec_ptrn)){
                return;
            }
            // HHmmss
            String cur_time_sec = MOMENT.substring(8,12); //hh:mm::ss HHmmss
            String time_from = conf.get("candle.time.from");
            String time_to = conf.get("candle.time.to");


            if (cur_time_sec.compareTo(time_to) > 0 || cur_time_sec.compareTo(time_from) < 0){
                return;
            }

            String cur_date = MOMENT.substring(0, 7);
            String date_from = conf.get("candle.date.from");
            String date_to = conf.get("candle.date.to");
            if (cur_date.compareTo(date_to) > 0 || cur_date.compareTo(date_from) < 0){
                return;
            }

            long width = Long.parseLong(conf.get("candle.width"));

            String cur_time_full = MOMENT.substring(8);

            long hrs = Long.parseLong(cur_time_full.substring(0, 2));
            long mins = Long.parseLong(cur_time_full.substring(2, 4));
            long sec = Long.parseLong(cur_time_full.substring(4, 6));
            long ms =  Long.parseLong(cur_time_full.substring(6));

            long time_ms = hrs*60*60*1000 + mins*60*1000 + sec*1000 + ms;

            long out_time_ms = (time_ms / width) * width;
            String CANDLE_MOMENT = cur_date + Long.toString(out_time_ms / (60 * 60 * 1000))
                                    + Long.toString(out_time_ms / (60 * 1000))
                                    + Long.toString(out_time_ms / (1000))
                                    + Long.toString(out_time_ms % (1000));

            endkey.set(SYMBOL + "," + CANDLE_MOMENT);
            endval.set(MOMENT + "," + PRICE + "," + ID );
            log.info(endkey.toString());
            log.info(endval.toString());

            context.write(endkey, endval);
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
        public static final Log log = LogFactory.getLog(IntSumReducer.class); // поле класса
        private Text res_key = new Text();
        private Text res_values = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            double LOW = -1.0, HIGH = -1.0, OPEN  = -1.0, CLOSE = -1.0;
            int id_open  = -1;
            int id_close = -1;
            int idx = 0;
            res_key = key;
            log.info("****");
            log.info(key.toString());

            for(Object value: values){
                log.info(value.toString());
                String[] value_split = value.toString().split(",");
                long val_moment =  Long.parseLong(value_split[0]);
                double val_price = Double.parseDouble((value_split[1]));
                int val_id = Integer.parseInt((value_split[2]));

                if(OPEN == -1.0 || OPEN > val_moment || (OPEN == val_moment && val_id < id_open)) {
                    id_open = val_id;
                    OPEN = val_price;
                }

                if(CLOSE == -1.0 || (CLOSE <= val_moment  || (CLOSE == val_moment && val_id > id_close)) ){
                    CLOSE = val_price;
                    id_close = val_id;
                }

                if(LOW == -1.0 || LOW > val_price){
                    LOW = val_price;
                }

                if(LOW == -1.0 || HIGH < val_price){
                    HIGH  = val_price;
                }
            }
            log.info("***");
            res_values.set(Double.toString(OPEN) + "," +  Double.toString(HIGH) + Double.toString(LOW) + Double.toString(CLOSE));
            context.write(res_key, res_values);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: candle <in> <out>");
            System.exit(2);
        }
        conf.setIfUnset("candle.width", "300000");
        conf.setIfUnset("candle.securities", ".*");
        conf.setIfUnset("candle.date.from", "19000101");
        conf.setIfUnset("candle.date.to", "20200101");
        conf.setIfUnset("candle.time.from", "1000");
        conf.setIfUnset("candle.time.to", "2300");
        conf.setIfUnset("candle.num.reducers", "1");


        Job job = new Job(conf, "candle");
       // job.setNumReduceTasks(conf.getInt("candle.num.reducers", 1));
        job.setJarByClass(candle.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(CSVHeaderFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}