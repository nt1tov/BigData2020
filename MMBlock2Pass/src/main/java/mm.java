import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import CSVReader.CSVHeaderFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;





public class mm {
    public static class MatrixBlockKey implements WritableComparable<MatrixBlockKey>{
        private LongWritable rowBlock;
        private LongWritable colBlock;
        private LongWritable groupIdx;

        public MatrixBlockKey(){
            rowBlock = new LongWritable(0);
            colBlock = new LongWritable(0);
            groupIdx = new LongWritable(0);
        }

        public MatrixBlockKey(long Row, long Column, long dup){
            rowBlock = new LongWritable(Row);
            colBlock = new LongWritable(Column);
            groupIdx = new LongWritable(dup);
        }

        public long getBlockRow(){
            return rowBlock.get();
        }

        public long getBlockCol(){
            return colBlock.get();
        }

        public long getGroupIdx(){
            return groupIdx.get();
        }

        @Override
        public int compareTo(MatrixBlockKey other) {
            int compareRow = rowBlock.compareTo(other.rowBlock);
            int compareCol = colBlock.compareTo(other.colBlock);
            int compareGroupIdx= colBlock.compareTo(other.groupIdx);

            if (compareRow == 0) {
                if (compareCol == 0) {
                    return compareGroupIdx;
                } else {
                    return compareCol;
                }
            } else {
                return compareRow;
            }
        }

        @Override
        public void write(DataOutput DataOutput) throws IOException{
            rowBlock.write(DataOutput);
            colBlock.write(DataOutput);
            groupIdx.write(DataOutput);
        }

        @Override
        public void readFields(DataInput Datainput)  throws IOException{
            rowBlock.readFields(Datainput);
            colBlock.readFields(Datainput);
            groupIdx.readFields(Datainput);
        }

    }

    public static class MatrixBlockValue implements Writable{
        private ObjectWritable matrixTag;
        private LongWritable colLocal;
        private LongWritable rowLocal;
        private DoubleWritable value;


        public MatrixBlockValue(){
            colLocal = new LongWritable(0);
            rowLocal = new LongWritable(0);
            value = new DoubleWritable(0.0);
            matrixTag = new ObjectWritable("");

        }

        public MatrixBlockValue(String tag, long row, long column, double val){
            matrixTag = new ObjectWritable(tag);
            rowLocal = new LongWritable(row);
            colLocal = new LongWritable(column);
            value = new DoubleWritable(val);
        }

        public Object getMatrixTag(){
            return matrixTag.get();
        }

        public long getRowLocal(){
            return rowLocal.get();
        }

        public long getBlockCol(){
            return colLocal.get();
        }

        public double getValue(){
            return value.get();
        }

        @Override
        public void write(DataOutput DataOutput) throws IOException{
            matrixTag.write(DataOutput);
            rowLocal.write(DataOutput);
            colLocal.write(DataOutput);
            value.write(DataOutput);
        }

        @Override
        public void readFields(DataInput Datainput)  throws IOException{
            matrixTag.readFields(Datainput);
            rowLocal.readFields(Datainput);
            colLocal.readFields(Datainput);
            value.readFields(Datainput);
        }

    }

    public static class BlockMapper extends Mapper<Object, Text, MatrixBlockKey, MatrixBlockValue> {

        public static final Log log = LogFactory.getLog(BlockMapper.class);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            int BLOCK_SIZE = conf.getInt("mm.groups", 1);
            long M_SIZE = conf.getLong("m_size", -1);
            long N_SIZE = conf.getLong("n_size", -1);
            long P_SIZE = conf.getLong("p_size", -1);

            assert (M_SIZE != -1 && N_SIZE != -1 && P_SIZE != -1);

            long LeftBlockColSize = M_SIZE / BLOCK_SIZE;
            long RightBlockRowSize = P_SIZE / BLOCK_SIZE;
            String[] input_record = value.toString().split("\t");
            String MatrixTag = input_record[0];
            long col =  Long.parseLong(input_record[1]);
            long row =  Long.parseLong(input_record[2]);
            double val = Double.parseDouble(input_record[3]);

            log.info(conf.get("mm.tags"));
            String LeftMatrixTag = "A";
            String RightMatrixTag = "B";

            MatrixBlockKey outKey;
            MatrixBlockValue outVal;

            if(MatrixTag.equals(LeftMatrixTag)){
                long blockColumns = P_SIZE / BLOCK_SIZE;
                for (int i = 0; i < blockColumns; ++i){
                    context.write(new MatrixBlockKey(row / BLOCK_SIZE, col / BLOCK_SIZE, i),
                                new MatrixBlockValue(MatrixTag, row % BLOCK_SIZE,
                                                    col % BLOCK_SIZE, val));
                }
            }
            else if(MatrixTag.equals(RightMatrixTag)){
                long blockRows = M_SIZE / BLOCK_SIZE;
                for (int i = 0; i < blockRows; ++i){
                    context.write(new MatrixBlockKey(i, row / BLOCK_SIZE, col / BLOCK_SIZE),
                            new MatrixBlockValue(MatrixTag, row % BLOCK_SIZE,
                                    col % BLOCK_SIZE, val));
                }
            }
            else{
                throw new InterruptedException();
            }


        }
    }



    public static class BlockReducer extends Reducer<Text, Text, Text, Text> {
        public static final Log log = LogFactory.getLog(BlockReducer.class);

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int BLOCK_SIZE = conf.getInt("mm.groups", 1);


        }
    }

    //public static class IdentityMapper extends Mapper
   // public static class BlockSumReducer


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: mm <A path> <B path> <C path>");
            System.exit(2);
        }
        conf.setIfUnset("mm.tags", "ABC");
        conf.setIfUnset("mm.float-format", "%.3f");
        conf.setIfUnset("mm.reduce.tasks", "1");
        String tags = conf.get("mm.tags");

        assert (tags.length() == 3);

        //parse matrix A size
        BufferedReader reader;
        Long m = -1L, n = -1L, p = -1L, o = -1L;
        try {
            reader = new BufferedReader(new FileReader(otherArgs[0] + "/size"));
            String[] size_str = reader.readLine().split("\t");
            m = Long.parseLong(size_str[0]);
            n = Long.parseLong(size_str[1]);
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        //parse matrix B size
        try {
            reader = new BufferedReader(new FileReader(otherArgs[1] + "/size"));
            String[] size_str = reader.readLine().split("\t");
            p =  Long.parseLong(size_str[0]);
            o =  Long.parseLong(size_str[1]);
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //check matrixes's sizes
        assert (n.equals(p));
        assert (n != -1 && m != -1 && o != -1);

        conf.set("m_size", m.toString());
        conf.set("n_size", n.toString());
        conf.set("o_size", o.toString());

        Job multBlocks = new Job(conf, "multBlocks");
        multBlocks.setNumReduceTasks(conf.getInt("mapred.reduce.tasks", 1));
        multBlocks.setJarByClass(mm.class);

        multBlocks.setInputFormatClass(TextInputFormat.class);
        multBlocks.setOutputFormatClass(TextOutputFormat.class);

        multBlocks.setMapperClass(BlockMapper.class);
        //multBlocks.setReducerClass(BlockReducer.class);

        multBlocks.setOutputKeyClass(MatrixBlockKey.class);
        multBlocks.setOutputValueClass(MatrixBlockValue.class);

        FileInputFormat.addInputPath(multBlocks, new Path(otherArgs[0]+ "/data") );
        FileInputFormat.addInputPath(multBlocks, new Path(otherArgs[1]+ "/data"));
        FileOutputFormat.setOutputPath(multBlocks, new Path("tmp"));



//        Job sumBlocks = new Job(conf, "secondStep");
//        sumBlocks.setJarByClass(mm.class);
//
////        sumJob.setMapperClass(SumMapper.class);
////        sumJob.setReducerClass(SumReducer.class);
//
//        sumBlocks.setInputFormatClass(TextInputFormat.class);
//        sumBlocks.setOutputFormatClass(TextOutputFormat.class);
//
//        sumBlocks.setOutputKeyClass(MatrixCoords.class);
//        sumBlocks.setOutputValueClass(DoubleWritable.class);
//
//        FileInputFormat.addInputPath(sumBlocks, new Path("tmp"));
//        FileOutputFormat.setOutputPath(sumBlocks, new Path(otherArgs[2] + "/data"));

        System.exit(multBlocks.waitForCompletion(true)
                /*&& sumBlocks.waitForCompletion(true) */ ? 0 : 1);
    }
}