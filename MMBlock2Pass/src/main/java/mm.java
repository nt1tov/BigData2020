import java.io.*;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import CSVReader.CSVHeaderFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;
import java.util.Locale;


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

    public static class MatrixIndexes implements WritableComparable<MatrixIndexes>{

        private LongWritable row;
        private LongWritable col;
        public MatrixIndexes(){
            row = new LongWritable(-1);
            col = new LongWritable(-1);
        }

        public MatrixIndexes(long rowIdx, long colIdx){
            row = new LongWritable(rowIdx);
            col = new LongWritable(colIdx);
        }

        public long getRow(){
            return row.get();
        }

        public long getCol(){
            return col.get();
        }

        @Override
        public String toString(){
            return String.format("%d %d", row.get(), col.get());
        }

        @Override
        public int compareTo(MatrixIndexes other){
            int comparedRows = row.compareTo(other.row);
            int comparedCols = col.compareTo(other.col);
            if(comparedRows == 0){
                return comparedCols;
            }
            else{
                return  comparedRows;
            }
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            row.write(dataOutput);
            col.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException{
            row.readFields(dataInput);
            col.readFields(dataInput);
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

        public long getColLocal(){
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

    public static class MultBlockMapper extends Mapper<Object, Text, MatrixBlockKey, MatrixBlockValue> {

        public static final Log log = LogFactory.getLog(MultBlockMapper.class);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            int BLOCK_SIZE = conf.getInt("mm.groups", 1);
            long M_SIZE = conf.getLong("m_size", -1);
            long N_SIZE = conf.getLong("n_size", -1);
            long P_SIZE = conf.getLong("p_size", -1);

            log.info("block_size=" + BLOCK_SIZE + " M_SIZE" + M_SIZE + " N_SIZE=" + N_SIZE + " P_size=" + P_SIZE);

            if( M_SIZE == -1 || N_SIZE == -1 || P_SIZE == -1){
                throw new IOException();
            }

            String[] input_record = value.toString().split("\t");
            log.info(input_record[0]);
            log.info(input_record[1]);
            log.info(input_record[2]);
            log.info(input_record[3]);

            String MatrixTag = input_record[0];
            long col =  Long.parseLong(input_record[1]);
            long row =  Long.parseLong(input_record[2]);
            double val = Double.parseDouble(input_record[3]);


            String LeftMatrixTag = conf.get("mm.tags").substring(0, 1);
            String RightMatrixTag = conf.get("mm.tags").substring(1, 2);;
//            log.info(LeftMatrixTag);
//            log.info(RightMatrixTag);
            MatrixBlockKey outKey;
            MatrixBlockValue outVal;

            if(MatrixTag.equals(LeftMatrixTag)){
                long blockColumns = P_SIZE / BLOCK_SIZE;
                log.info("blockColumns="+ blockColumns);
                for (int i = 0; i < blockColumns; ++i){
                    context.write(new MatrixBlockKey(row / BLOCK_SIZE, col / BLOCK_SIZE, i),
                                new MatrixBlockValue(MatrixTag, row % BLOCK_SIZE,
                                                    col % BLOCK_SIZE, val));
                }
            }
            else if(MatrixTag.equals(RightMatrixTag)){
                long blockRows = M_SIZE / BLOCK_SIZE;
                log.info("blockRows="+ blockRows);
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



    public static class MultBlockReducer extends Reducer<MatrixBlockKey, MatrixBlockValue, MatrixIndexes, DoubleWritable> {
        public static final Log log = LogFactory.getLog(MultBlockReducer.class);
        public static final double epsilon = Math.pow(10, -9);

        @Override
        public void reduce(MatrixBlockKey key, Iterable<MatrixBlockValue>  values, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            int BLOCK_SIZE = conf.getInt("mm.groups", 1);
            long rowShift = key.getBlockRow();
            long colShift = key.getGroupIdx();

            String LeftMatrixTag = conf.get("mm.tags").substring(0, 1);
            String RightMatrixTag = conf.get("mm.tags").substring(1, 2);

            double[][] leftBlock = new double[BLOCK_SIZE][BLOCK_SIZE];
            double[][] rightBlock = new double[BLOCK_SIZE][BLOCK_SIZE];

            // fill block arrs from values
            for (MatrixBlockValue value : values) {
                int i = (int) value.getRowLocal();
                int j = (int) value.getColLocal();
                if (value.getMatrixTag().toString().equals(LeftMatrixTag)) {
                    leftBlock[i][j] = value.getValue();
                }
                else if(value.getMatrixTag().toString().equals(RightMatrixTag)) {
                    rightBlock[i][j] = value.getValue();
                }
                else{
                    throw new IOException();
                }
            }

            //multiply Left x Right block; write resulrs to context

            for (int i = 0; i < BLOCK_SIZE; ++i) {
                for (int j = 0; j < BLOCK_SIZE; ++j) {
                    double outputElemPart = 0.0;
                    for (int k = 0; k < BLOCK_SIZE; ++k) {
                        outputElemPart += leftBlock[i][k] * rightBlock[k][j];
                    }
                    if(Math.abs(outputElemPart) < epsilon){
                        continue;
                    }
                    context.write(new MatrixIndexes(rowShift + i, colShift + j), new DoubleWritable(outputElemPart));
                }
            }
        }

    }

    public static class IdentityMapper extends Mapper<Object, Text,  MatrixIndexes, DoubleWritable> {

        public static final Log log = LogFactory.getLog(MultBlockMapper.class);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();

            int BLOCK_SIZE = conf.getInt("mm.groups", 1);
            long M_SIZE = conf.getLong("m_size", -1);
            long N_SIZE = conf.getLong("n_size", -1);
            long P_SIZE = conf.getLong("p_size", -1);

            String[] splittedValues = value.toString().split("\\s+");
            log.info("Identuty Mapper: " + value.toString());
            long row = Long.parseLong(splittedValues[0]);
            long col = Long.parseLong(splittedValues[1]);

            double valueofMatrix = Double.parseDouble(splittedValues[2]);
            context.write(new MatrixIndexes(row, col), new DoubleWritable(valueofMatrix));
        }
    }

    public static class SumReducer extends Reducer<MatrixIndexes, Iterable<DoubleWritable>, NullWritable, Text> {
        public static final Log log = LogFactory.getLog(MultBlockReducer.class);

        @Override
        public void reduce(MatrixIndexes key, Iterable<DoubleWritable>  values, Context context) throws IOException, InterruptedException {

            double outputValueFull = 0;
            for (DoubleWritable value : values) {
                outputValueFull += value.get();
            }
            Configuration conf = context.getConfiguration();
            String valueFormat = conf.get("mm.float-format", "%.3f");
            String OutputTag = conf.get("mm.tags").substring(2, 3);
            log.info("OUTPUTTAG=" + OutputTag);
            String valueFormatted = String.format(Locale.ENGLISH, valueFormat, outputValueFull);
            String outputStr =  OutputTag + "\t" +  key.getRow()  + "\t" +  key.getCol()  + "\t" + valueFormatted;
            context.write(NullWritable.get(), new Text(outputStr));

        }

    }

    //public static class IdentityMapper extends Mapper
   // public static class BlockSumReducer


    public static void main(String[] args) throws Exception {
        final Log log = LogFactory.getLog(mm.class);
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: mm <A path> <B path> <C path>");
            System.exit(2);
        }
        conf.setIfUnset("mm.tags", "ABC");
        conf.setIfUnset("mm.float-format", "%.3f");
        conf.setIfUnset("mapred.reduce.tasks", "1");
        conf.setIfUnset("mm.groups", "1");
        String tags = conf.get("mm.tags");

        assert (tags.length() == 3);

        //parse matrix A size
        BufferedReader reader;
        long m = -1L, n = -1L, p = -1L, o = -1L, n1 = -1L;
        try {
            reader = new BufferedReader(new FileReader(otherArgs[0] + "/size"));
            String input_line = reader.readLine();
            String[] size_str = input_line.split("\t");
            log.info(input_line);
            m = Long.parseLong(size_str[0]);
            n = Long.parseLong(size_str[1]);
            log.info(size_str);
            reader.close();
        } catch (IOException e) {
            System.out.println("exception Left");
            e.printStackTrace();
            System.exit(1);
        }
        //parse matrix B size
        try {
            reader = new BufferedReader(new FileReader(otherArgs[1] + "/size"));
            String input_line = reader.readLine();
            String[] size_str = input_line.split("\t");
            log.info(input_line);
            n1 =  Long.parseLong(size_str[0]);
            p =  Long.parseLong(size_str[1]);
            log.info(size_str);
            reader.close();
        } catch (IOException e) {
            System.out.println("exception right");
            e.printStackTrace();
            System.exit(1);
        }

        //check matrixes's sizes
        assert (n == n1);
        assert (n != -1 && m != -1 && p != -1);

        conf.set("m_size", Long.toString(m));
        conf.set("n_size", Long.toString(n));
        conf.set("p_size", Long.toString(p));

        Job multBlocks = new Job(conf, "multBlocks");
        multBlocks.setNumReduceTasks(conf.getInt("mapred.reduce.tasks", 1));
        multBlocks.setJarByClass(mm.class);

        multBlocks.setInputFormatClass(TextInputFormat.class);
        multBlocks.setOutputFormatClass(TextOutputFormat.class);

        multBlocks.setMapperClass(MultBlockMapper.class);
        multBlocks.setReducerClass(MultBlockReducer.class);

        multBlocks.setMapOutputKeyClass(MatrixBlockKey.class);
        multBlocks.setMapOutputValueClass(MatrixBlockValue.class);

        multBlocks.setOutputKeyClass(Text.class);
        multBlocks.setOutputValueClass(Text.class);


        String pathLeft = otherArgs[0]+ "/data";
        String pathRight = otherArgs[1]+ "/data";
        FileInputFormat.addInputPaths(multBlocks, pathLeft + "," + pathRight);
        FileOutputFormat.setOutputPath(multBlocks, new Path("tmp"));

        Job sumBlocks = new Job(conf, "sumBlocks");
        sumBlocks.setJarByClass(mm.class);

        sumBlocks.setMapperClass(IdentityMapper.class);
        sumBlocks.setReducerClass(SumReducer.class);

        sumBlocks.setMapOutputKeyClass(MatrixIndexes.class);
        sumBlocks.setMapOutputValueClass(DoubleWritable.class);

        sumBlocks.setInputFormatClass(TextInputFormat.class);
        sumBlocks.setOutputFormatClass(TextOutputFormat.class);

        sumBlocks.setOutputKeyClass(NullWritable.class);
        sumBlocks.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(sumBlocks, new Path("tmp"));
        FileOutputFormat.setOutputPath(sumBlocks, new Path(otherArgs[2] + "/data"));



        System.exit(multBlocks.waitForCompletion(true)
                && sumBlocks.waitForCompletion(true)  ? 0 : 1);
    }
}