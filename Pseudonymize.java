import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.hadoop.fs.FSDataInputStream;

import java.security.MessageDigest;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import org.json.simple.JSONArray;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.lang.Math;


import java.util.Base64;
import java.io.FileInputStream;
import java.net.URI;
import java.util.Random;
import java.util.HashMap;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Arrays;

import java.util.Map;
import java.io.FileReader;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.time.Duration;
import java.time.Instant;

public class Pseudonymize {
    public static void main(String[] args) throws Exception {
        long startTime = System.currentTimeMillis(); // 작업시간 기록

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        cleanupTemporaryDirectory(fs, new Path("/user/hadoopuser/tmp"));

        String columns = readFirstLineFromFirstFile(fs, args[0]);
        List<String> tableColumnNameList = Arrays.asList(columns.split(","));
        conf.set("tableColumnNameList", String.join(",", tableColumnNameList));

        JSONObject jsonObject = readJsonFile(fs, new Path(args[1]));
        List<String> columnNameList = extractColumnNames(jsonObject);
        List<String> columnNumberList = extractColumnNumbers(tableColumnNameList, columnNameList);
        List<String> algorithms = new ArrayList<>();
        List<List<String>> options = new ArrayList<>();
        List<String> preprocessingOptions = new ArrayList<>(); // 전처리 옵션 저장 리스트
        List<String> preprocessingTypes = new ArrayList<>();
        boolean preprocessingEnabled = false;
        
        if (jsonObject.containsKey("전처리")) {
            JSONArray preprocessingArray = (JSONArray) jsonObject.get("전처리");
            preprocessingOptions = new ArrayList<>(preprocessingArray);
            preprocessingEnabled = true;
            preprocessingTypes = preprocessingOptions.subList(1, preprocessingOptions.size()); // 전처리 옵션들 저장
            conf.setBoolean("preprocessingEnabled", true);
            conf.setStrings("preprocessingTypes", preprocessingTypes.toArray(new String[0]));
        } else {
            conf.setBoolean("preprocessingEnabled", false);
        }

        processJsonColumns(jsonObject, columnNameList, algorithms, options);
        String formattedOptions = formatOptions(options);
        setConfiguration(conf, tableColumnNameList, columnNumberList, algorithms, formattedOptions);

        if (conf.getBoolean("preprocessingEnabled", false)) {
            System.out.println(String.join(", ", preprocessingTypes) + " start!");
            hadoopConfiguration_step0(args, conf);
            System.out.println("---------------------------------------------");
            for (String preprocessingType : preprocessingTypes) {
                System.out.println("컬럼: " + columnNameList.get(0));
                System.out.println("수행 기능: " + preprocessingType);
                printPreprocessorOutput(fs, args[2], preprocessingType);
                System.out.println("---------------------------------------------");
            }
            
        } else {
            System.out.println(String.join(", ", algorithms) + " start!");
            if (algorithms.contains("Aggregation") || algorithms.contains("TopBottom") || algorithms.contains("Random")) {
                hadoopConfiguration_step2(args, conf);
            } else {
                hadoopConfiguration_step1(args, conf);
            }

            System.out.println("---------------------------------------------");
            for (int i = 0; i < columnNameList.size(); i++) {
                System.out.println("컬럼: " + columnNameList.get(i));
                System.out.println("수행 기능: " + algorithms.get(i));
                System.out.println("---------------------------------------------");
            }
        }

        printExecutionTime(startTime);
        System.out.println("hdfs 저장완료: " + args[2]);
    }

    
    private static List<String> extractColumnNames(JSONObject jsonObject) {
        List<String> columnNameList = new ArrayList<>();
        for (Object key : jsonObject.keySet()) {
            if (!key.equals("전처리")) {
                columnNameList.add((String) key);
            }
        }
        return columnNameList;
    } 

    private static void setConfiguration(Configuration conf, List<String> tableColumnNameList, List<String> columnNumberList, List<String> algorithms, String formattedOptions) {
        conf.set("tableColumnNameList", String.join(",", tableColumnNameList));
        conf.set("columnNumberList", String.join(",", columnNumberList));
        conf.set("algorithms", String.join(",", algorithms));
        conf.set("options", formattedOptions);
    }

    private static String readFirstLineFromFirstFile(FileSystem fs, String inputPath) throws Exception {

        // readFiestLineFromFirstFile(): HDFS에서 주어진 경로의 첫 번째 파일을 열고, 그 파일의 첫 번째 줄을 읽어 반환

        Path path = new Path(inputPath);
        FileStatus[] fileStatuses = fs.listStatus(path);

        if (fileStatuses.length > 0) {  // 해당 경로에 파일이 하나라도 있다면,
            Path filePath = fileStatuses[0].getPath();   // 첫 번째 파일의 경로를 가져온다. ex. tableA/small_0.csv
            try (FSDataInputStream fsDataInputStream = fs.open(filePath);
                 BufferedReader br = new BufferedReader(new InputStreamReader(fsDataInputStream))) {
                fsDataInputStream.seek(0); // 파일의 처음 위치로 이동
                String line;
                if ((line = br.readLine()) != null) {
                    return line; // 파일에서 읽은 한 줄 저장해서 반환
                }
            }
        }
        return null;
    }
    
    private static void printPreprocessorOutput(FileSystem fs, String outputPathStr, String preprocessingType) throws IOException {
        FileStatus[] fileStatuses = fs.listStatus(new Path(outputPathStr));
        for (FileStatus fileStatus : fileStatuses) {
            try (FSDataInputStream in = fs.open(fileStatus.getPath());
                 BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                String line;
                while ((line = br.readLine()) != null) {
                    // 라인을 MinMaxTuple의 형식으로 파싱하여 필요한 값만 출력
                    String[] parts = line.trim().split("\\s+|,");
                    if (parts.length > 2) {
                        double min = Double.parseDouble(parts[1].trim());
                        double max = Double.parseDouble(parts[2].trim());
                            
                        switch (preprocessingType) {
                            case "Avg":
                                System.out.println("평균값: " + min);
                                break;
                            case "Sd":
                                System.out.println("표준편차: " + max);
                                break;
                            case "Min":
                                System.out.println("최솟값: " + min);
                                break;
                            case "Max":
                                System.out.println("최댓값: " + max);
                                break;
                        }
                    } else {
                        System.err.println("Invalid data format: " + line);
                    }
                }
            }
        }
    }
    
    private static void printExecutionTime(long startTime) {
        long endTime = System.currentTimeMillis(); // 작업 종료 시간 기록
        long elapsedTime = endTime - startTime;
        long elapsedMinutes = elapsedTime / 60000; // 밀리초를 분으로 변환
        long remainingSeconds = (elapsedTime % 60000) / 1000; // 남은 밀리초를 초로 변환
        System.out.println("작업 실행 시간: " + elapsedTime + " ms (" + elapsedMinutes + " 분 " + remainingSeconds + " 초)"); // 실행 시간 출력
    }

    private static void setupConfiguration(Configuration conf) {

        conf.set("mapreduce.job.jvm.numtasks", "-1"); 
        conf.set("mapreduce.map.cpu.vcores", "1"); 
        conf.set("mapreduce.reduce.cpu.vcores", "1"); 
        
        conf.set("mapred.compress.map.output", "true");
        conf.set("mapred.output.compression.type", "BLOCK");
        conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        conf.set("yarn.app.mapreduce.am.resource.mb", "35840 ");
        conf.set("mapreduce.tasktracker.map.tasks.maximum", "8"); 
        conf.set("mapreduce.map.memory.mb", "2048");  
        conf.set("mapreduce.reduce.memory.mb", "2048");  
        conf.set("mapreduce.task.io.sort.mb","200");

        conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(128 * 1024 * 1024)); 
        conf.set("mapreduce.input.fileinputformat.split.minsize", String.valueOf(128 * 1024 * 1024));  
        conf.set("dfs.block.size", String.valueOf(128 * 1024 * 1024));
    }


    private static Job setupJob1(Configuration conf, String[] args) throws IOException, URISyntaxException { //PretrainMapper-Reducer
        
        Job job1 = new Job(conf);
        job1.addCacheFile(new URI(args[1]+"#Benchmark.json"));
        job1.setJarByClass(Pseudonymize.class);
        job1.setMapperClass(PseudonymizePreTrainMapper.class);
        job1.setCombinerClass(PseudonymizePreTrainCombiner.class);
        job1.setReducerClass(PseudonymizePreTrainReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(MinMaxTuple.class);
        return job1;

    }

    private static Job setupJob2(Configuration conf, FileSystem hdfs, Path tmp_output, String[] args) throws IOException, URISyntaxException { //mapper 
        
        Job job2 = new Job(conf); 
            
        FileStatus[] fileStatus = hdfs.listStatus(tmp_output);
        for (FileStatus file : fileStatus) {  
            String[] filenames = file.getPath().toString().split("/");
            String filename = "tmp/" + filenames[filenames.length - 1];   
            job2.addCacheFile(new URI(filename+"#"+filename));          
        }
        
        job2.addCacheFile(new URI("tmp#tmp"));
        job2.addCacheFile(new URI(args[1]+"#Benchmark.json"));
        job2.setJarByClass(Pseudonymize.class);
        job2.setNumReduceTasks(0);
        job2.setMapperClass(PseudonymizeHadoopMapper.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);
        
        return job2;
    }

    private static Job setupJob(Configuration conf, String[] args) throws IOException, URISyntaxException { //mapper
        Job job = Job.getInstance(conf, "JOB_1");
        job.addCacheFile(new URI(args[1] + "#Benchmark.json"));
        job.setNumReduceTasks(0);
        job.setJarByClass(Pseudonymize.class);
        job.setMapperClass(PseudonymizeHadoopMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        return job;
    }

    private static void cleanupTemporaryDirectory(FileSystem hdfs, Path tmp_output) throws IOException {
        if (hdfs.exists(tmp_output)) {
            hdfs.delete(tmp_output, true);
        }
    }

    public static void hadoopConfiguration_step0(String[] args, Configuration conf) throws Exception {

        setupConfiguration(conf);

        FileSystem hdfs = FileSystem.get(conf);
        Job job1 = setupJob1(conf, args);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        if (!job1.waitForCompletion(true)) {
            throw new RuntimeException("Step 0 failed");
        }
    }

    public static void hadoopConfiguration_step2(String[] args, Configuration conf) throws Exception {

        setupConfiguration(conf);

        FileSystem hdfs = FileSystem.get(conf);
        Job job1 = setupJob1(conf, args);
        Path tmp_output = new Path("tmp");

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, tmp_output);

        if (job1.waitForCompletion(true)) {
            Job job2 = setupJob2(conf, hdfs, tmp_output, args);
            FileInputFormat.addInputPath(job2, new Path( args[0]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            FileInputFormat.setInputDirRecursive(job2, true);

            boolean job2Completed = job2.waitForCompletion(true);
            //cleanupTemporaryDirectory(hdfs, tmp_output);

            if (!job2.waitForCompletion(true)) {
                throw new RuntimeException("Step 2 failed");
            }
            
        }
    }

    public static void hadoopConfiguration_step1(String[] args, Configuration conf) throws Exception {
        
        setupConfiguration(conf);

        FileSystem hdfs = FileSystem.get(conf);
        Job job = setupJob(conf, args);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileInputFormat.setInputDirRecursive(job, true); 

        if (!job.waitForCompletion(true)) {
            throw new RuntimeException("Step 1 failed");
        }
    }

    private static JSONObject readJsonFile(FileSystem fs, Path path) throws IOException, ParseException {
        JSONParser parser = new JSONParser();
        try (InputStream is = fs.open(path);
             InputStreamReader isr = new InputStreamReader(is, "utf-8")) {
            return (JSONObject) parser.parse(isr);
        }
    }

    private static List<String> extractColumnNumbers(List<String> tableColumnNameList, List<String> columnNameList) {
        List<String> columnNumberList = new ArrayList<>();
        for (String columnName : columnNameList) {
            if (tableColumnNameList.contains(columnName)) {
                int colNum = tableColumnNameList.indexOf(columnName);
                columnNumberList.add(String.valueOf(colNum));
            }
        }
        return columnNumberList;
    }

    private static void processJsonColumns(JSONObject jsonObject, List<String> columnNameList, List<String> algorithms, List<List<String>> options) {
        for (String colName : columnNameList) {
            JSONArray valueArray = (JSONArray) jsonObject.get(colName);
            algorithms.add((String) valueArray.get(0));

            List<String> optionList = new ArrayList<>();
            for (Object value : valueArray) {
                optionList.add((String) value);
            }
            options.add(optionList);
        }
    }

    private static String formatOptions(List<List<String>> options) {
        List<String> optionsStrings = new ArrayList<>();
        for (List<String> innerList : options) {
            optionsStrings.add(String.join(",", innerList));
        }
        return String.join(";", optionsStrings);
    }

    public static class MinMaxTuple implements Writable{ 

        private Double min = Double.MAX_VALUE;
        private Double max = - Double.MAX_VALUE;
    
        public Double getMin() {
            return min;
        }
        public void setMin(Double min) {
            this.min = min;
        }
    
        public Double getMax() {
            return max;
        }   
        public void setMax(Double max) {
            this.max = max;
        }
    
        public void readFields(DataInput in) throws IOException { 
            min = in.readDouble();
            max = in.readDouble();
        }
    
        public void write(DataOutput out) throws IOException {
            out.writeDouble(min);
            out.writeDouble(max);
        }
    
        public String toString() {
            return min + "," + max + ",";
        }
    }
    
    public static class PseudonymizePreTrainMapper extends Mapper<Object, Text, Text, Text>{

        List<Integer> columnNumberList = new ArrayList<>(); 
        List<String> algorithms = new ArrayList<>();   
        List<List<String>> options = new ArrayList<>();
        List<String> tableColumnNameList = new ArrayList<>();   

        @Override
        protected void setup(Context context) throws IOException, InterruptedException { 

            Configuration conf = context.getConfiguration();

            String tableColumnNameListStr = conf.get("tableColumnNameList");
            tableColumnNameList = Arrays.asList(tableColumnNameListStr.split(","));

            String columnNumberListStr = conf.get("columnNumberList");
            String[] columnNumberListArray = columnNumberListStr.split(",");
            columnNumberList = new ArrayList<>();

            for (String numStr : columnNumberListArray) {
                int num = Integer.parseInt(numStr);
                columnNumberList.add(num);
            }

            String algorithmsStr = conf.get("algorithms");
            algorithms = Arrays.asList(algorithmsStr.split(","));

            String optionsStr = conf.get("options");
            String[] optionsStrings = optionsStr.split(";");
            options = new ArrayList<>();

            for (String option : optionsStrings) {
                List<String> innerOptions = Arrays.asList(option.split(","));
                options.add(innerOptions);
            }

        }

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
            String[] values = value.toString().split(",");

            for (int columnIndex : columnNumberList) {
                String algorithm = algorithms.get(columnNumberList.indexOf(columnIndex));

                switch (algorithm) {
                    case "Aggregation":
                        handleAggregation(columnIndex, values, context);
                        break;
                    case "TopBottom":
                    case "Random":
                        handleTopBottomAndRandom(algorithm, columnIndex, values, context);
                        break;
                }
            }
        }

        private void handleAggregation(int columnIndex, String[] values, Context context) throws IOException, InterruptedException {
            Map<Integer, String> filterConditions = extractFilterConditions(columnIndex);
            if (allConditionsMatch(filterConditions, values)) {
                context.write(new Text("A" + columnIndex), new Text(values[columnIndex]));
            }
        }

        private Map<Integer, String> extractFilterConditions(int columnIndex) {
            Map<Integer, String> filterConditions = new HashMap<>();
            List<String> currentOptions = options.get(columnNumberList.indexOf(columnIndex));

            for (int j = 1; j < currentOptions.size(); j++) {
                String[] condition = currentOptions.get(j).split("=");
                filterConditions.put(tableColumnNameList.indexOf(condition[0]), condition[1]);
            }
            return filterConditions;
        }

        private boolean allConditionsMatch(Map<Integer, String> filterConditions, String[] values) {
            for (Map.Entry<Integer, String> entry : filterConditions.entrySet()) {
                if (!values[entry.getKey()].equals(entry.getValue())) {
                    return false;
                }
            }
            return true;
        }

        private void handleTopBottomAndRandom(String algorithm, int columnIndex, String[] values, Context context) throws IOException, InterruptedException {
            if (algorithm.equals("Random")) {
                try {
                    Double.parseDouble(values[columnIndex]);
                } catch (NumberFormatException e) {
                    return;
                }
            }
            context.write(new Text(algorithm.charAt(0) + String.valueOf(columnIndex)), new Text(values[columnIndex]));
        }
    }     

    public static class PseudonymizePreTrainCombiner extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            char method = (char) key.charAt(0);

            switch (method) {
                case 'A':
                case 'T':
                    processSumAndCount(key, values, context);
                    break;
                case 'R':
                    processMinMax(key, values, context);
                    break;
            }
        }

        private void processSumAndCount(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;
            double sumOfSquares = 0;

            for (Text val : values) {
                Double doubleValue = parseDouble(val);
                if (doubleValue != null) {
                    count++;
                    sum += doubleValue;
                    sumOfSquares += Math.pow(doubleValue, 2);
                } else {
                    System.out.println("Invalid number format: " + val);
                }
            }
            String combinedResult = String.format("%f,%d,%f", sum, count, sumOfSquares);
            context.write(key, new Text(combinedResult));
        }

        private void processMinMax(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Double minValue = Double.MAX_VALUE;
            Double maxValue = Double.MIN_VALUE;

            for (Text val : values) {
                Double doubleValue = parseDouble(val);
                if (doubleValue != null) {
                    minValue = Math.min(minValue, doubleValue);
                    maxValue = Math.max(maxValue, doubleValue);
                }
            }
            String minMaxResult = String.format("%f,%f", minValue, maxValue);
            context.write(key, new Text(minMaxResult));
        }

        private Double parseDouble(Text val) {
            try {
                return Double.parseDouble(val.toString());
            } catch (NumberFormatException e) {
                System.err.println("Invalid number format: " + val);
                return null;
            }
        }
    }

    
    public static class PseudonymizePreTrainReducer extends Reducer<Text, Text, Text, MinMaxTuple>{
        
        private final MinMaxTuple output = new MinMaxTuple();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            char method = (char) key.charAt(0);
            String column = key.toString().substring(1);

            switch (method) {
                case 'A':
                case 'T':
                    processAggregationOrTopBottom(values, column, context);
                    break;
                case 'R':
                    processRandomize(values, column, context);
                    break;
            }

            // MinMaxTuple 값 출력
            System.out.println("Min Value: " + output.getMin());
            System.out.println("Max Value: " + output.getMax());
        }

        private void processAggregationOrTopBottom(Iterable<Text> values, String column, Context context) throws IOException, InterruptedException {

            int totalCount = 0;
            double totalSum = 0;
            double totalSumOfSquares = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                try {
                    double sum = Double.parseDouble(parts[0]);
                    int count = Integer.parseInt(parts[1]);
                    double sumOfSquares = Double.parseDouble(parts[2]);

                    totalCount += count;
                    totalSum += sum;
                    totalSumOfSquares += sumOfSquares;
                } catch (Exception e) {
                    System.err.println("Invalid format: " + val.toString());
                }
            }

            double mean = totalSum / totalCount;
            double variance = (totalSumOfSquares / totalCount) - Math.pow(mean, 2);        
            double sd = Math.sqrt(variance);

            output.setMin(mean);
            output.setMax(sd);
            context.write(new Text(column), output);
            
        }

        private void processRandomize(Iterable<Text> values, String column, Context context) throws IOException, InterruptedException {
            double min = Double.MAX_VALUE;
            double max = -Double.MAX_VALUE;

            for (Text val : values) {
                String[] nums = val.toString().split(",");
                try {
                    min = Math.min(min, Double.parseDouble(nums[0]));
                    max = Math.max(max, Double.parseDouble(nums[1]));
                } catch (NumberFormatException e) {
                    System.err.println("Invalid number format: " + val);
                }
            }

            if (min != Double.MAX_VALUE || max != -Double.MAX_VALUE) {
                output.setMin(min);
                output.setMax(max);
                context.write(new Text(column), output);
            }
        }
    }


    public static class PseudonymizeHadoopMapper extends Mapper<Object, Text, NullWritable, Text>{


        static HashMap<Text, ArrayList<Double>> Map = new HashMap<Text, ArrayList<Double>>();
        ArrayList<String> list = new ArrayList<String>();

        List<Integer> columnNumberList = new ArrayList<>();  
        List<String> columnNameList = new ArrayList<>();  
        List<String> algorithms = new ArrayList<>();   
        List<List<String>> options = new ArrayList<>();
        List<String> tableColumnNameList = new ArrayList<>();   

        private Map<Integer, List<String[]>> filterConditions = new HashMap<>();

        private boolean isFirstLine = true; 

        @Override
        protected void setup(Context context) throws IOException, InterruptedException { 

            Configuration conf = context.getConfiguration();

            String tableColumnNameListStr = conf.get("tableColumnNameList");
            tableColumnNameList = Arrays.asList(tableColumnNameListStr.split(","));


            String columnNumberListStr = conf.get("columnNumberList");
            String[] columnNumberListArray = columnNumberListStr.split(",");
            columnNumberList = new ArrayList<>();

            for (String numStr : columnNumberListArray) {
                int num = Integer.parseInt(numStr);
                columnNumberList.add(num);
            }

            String algorithmsStr = conf.get("algorithms");
            algorithms = Arrays.asList(algorithmsStr.split(","));

            String optionsStr = conf.get("options");
            String[] optionsStrings = optionsStr.split(";");
            options = new ArrayList<>();

            for (String option : optionsStrings) {
                List<String> innerOptions = Arrays.asList(option.split(","));
                options.add(innerOptions);
            }

            for (int i = 0; i < algorithms.size(); i++) {
                if ("Aggregation".equals(algorithms.get(i))) {
                    List<String> currentOptions = options.get(i);
                    int colIndex = columnNumberList.get(i);

                    if (!filterConditions.containsKey(colIndex)) {
                        filterConditions.put(colIndex, new ArrayList<String[]>());
                    }

                    for (int j = 1; j < currentOptions.size(); j++) {
                        String[] condition = currentOptions.get(j).split("=");
                        int conditionColIndex = tableColumnNameList.indexOf(condition[0]);
                        filterConditions.get(colIndex).add(new String[]{String.valueOf(conditionColIndex), condition[1]});
                    }
                }
            }

            final File[] folder = new File("tmp").listFiles();  
            for (final File fileEntry : folder) {  
                if(!fileEntry.isDirectory()) { 
                    String filename = "tmp/" + fileEntry.getName(); 
                    BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(filename), "utf-8")); 
                    String line = null;  
        
                    while( (line = bufferedReader.readLine()) != null)  { 
                        try {
                            ArrayList<Double> list = new ArrayList<Double>();
                            String[] values = line.split("\\s+|,");
                            if (values.length >= 3) {
                                list.add(Double.parseDouble(values[1]));  
                                list.add(Double.parseDouble(values[2])); 
                                Map.put(new Text(values[0]), list);  
                            } else {
                                System.err.println("Invalid line: " + line);
                            }
                        } catch (NumberFormatException e) {
                            System.err.println("Failed to parse line: " + line);
                        }
                    }
                    bufferedReader.close();  
                }
            }
        }

        
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException {
            
            if (isFirstLine) {
                context.write(NullWritable.get(), value); 
                isFirstLine = false; 
                return;
            }

            String[] values = value.toString().split(",");
            StringBuilder result = new StringBuilder();

            try {
                for (int i = 0; i < values.length; i++) {
                    String val = processValueByAlgorithm(i, values);
                    if (val != null){
                        result.append(val);
                        if (i < values.length - 1) {
                            result.append(", "); 
                        }
                    }
                }
                context.write(NullWritable.get(), new Text(result.toString()));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }  

        private String processValueByAlgorithm(int index, String[] values) {

            String result = "";
            String option;

            try {
                if (columnNumberList.contains(index)) {

                    int colNum = columnNumberList.indexOf(index);
                    String algorithm = algorithms.get(colNum);
                    

                    switch (algorithm) {
                        case "Masking":
                            option = options.get(colNum).get(1);
                            result =  Masking(values[index], option, "*");
                            break;
                        case "PartDelete":
                            option = options.get(colNum).get(1);
                            result = PartDelete(values[index], option, ":","");
                            break;
                        case "Delete":
                            return null;

                        case "Round":
                            option = options.get(colNum).get(1);
                            int option2 = Integer.parseInt(options.get(colNum).get(2));
                            result = Rounding(values[index], option, option2);
                            break;
                        case "Encryption":
                            result = Encryption(values[index]);
                            break;
                        case "MicroAggregation":
                            result = processAggregation(index, values);
                            break;
                        case "TopBottom":
                            result = processTopBottom(index, values);
                            break;
                        case "Random":
                            result = processRandom(index, values);
                            break;
                    }
                } else {
                    result = values[index];
                }
            } catch (Exception  e) {
                e.printStackTrace();
            }

            return result;
                
        }

        private String processRandom(int index, String[] values){

            Text text_i = new Text(String.valueOf(index));
            ArrayList<Double> key_values = Map.get(text_i);

            try {
                Double.parseDouble(values[index]);
                Double min = key_values.get(0);
                Double max = key_values.get(1);
                return Randomize(values[index], min, max);
            } catch (NumberFormatException e) {
                return Randomize(values[index], 0.0, 0.0);
            }

        }
        

        private String processTopBottom(int index, String[] values) {
            
            Text text_i_T = new Text(String.valueOf(index));
            ArrayList<Double> key_values_T = Map.get(text_i_T);

            Double mean = key_values_T.get(0);  
            Double sd3 = key_values_T.get(1);
            return TopBottom(values[index], mean, sd3);

        }


        private String processAggregation(int index, String[] values) {

            List<String[]> conditions = filterConditions.get(index);

            if (conditions != null) {
                for (String[] condition : conditions) {
                    int conditionColIndex = Integer.parseInt(condition[0]);
                    if (!values[conditionColIndex].equals(condition[1])) {
                        return values[index];
                    }
                }
            }
            
            Text text_i_A = new Text(String.valueOf(index));
            ArrayList<Double> key_values_A = Map.get(text_i_A);

            Double mean = key_values_A.get(0);   
            Double sd3 = key_values_A.get(1);
            return TopBottom(values[index], mean, sd3);

        }

        public String Masking(String str, String _input, String mark){

            String[] _inputList = _input.split(":"); 
            
            String[] idxList = new String[_inputList.length];
        
            int flag = 0;
            for (int i = 0; i < _inputList.length; i++) {

                if ( _inputList[i].equals("0") || _inputList[i].equals("1")) {
                    if (flag == 0) {
                        idxList[i] = Integer.toString(i);
                    }
                        
                    else {
                        idxList[i] = Integer.toString(i-_inputList.length+str.length());
                    }
                }
                else {
                    idxList[i] = Integer.toString(i);
                    flag = 1;
                }     
            }
            int idx = 0;
            String result = "*";
            int flag2 = 0;
            for (int i = 0; i < str.length(); i++) {
                try {
                    if (Integer.toString(i).equals(idxList[idx])) {
                        if (_inputList[idx].equals("1") || _inputList[idx].equals("1-")) {
                            result += str.substring(i, i+1);
                            flag2 = 1;
                        }
                        else {
                            result += mark;
                            flag2 = 0;
                        }
                        idx++;
                    }
                    else {
                        if (flag2==1) {
                            result += str.substring(i, i+1);
                        }
                        else { result += mark; }
                        
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                    if (flag2==1) { result += str.substring(i, i+1); }
                    else { result += mark; }
                }
            }
            return result;
        }

        public String PartDelete(String str, String _input, String separator, String mark){
            String[] data = str.split(separator);
       
            String[] _inputList = _input.split(":"); 
            
            String[] idxList = new String[_inputList.length];
        
            int flag = 0;
            for (int i = 0; i < _inputList.length; i++) {
                if ( _inputList[i].equals("0") || _inputList[i].equals("1")) {
                    if (flag == 0) {
                        idxList[i] = Integer.toString(i);
                    }
                        
                    else {
                        idxList[i] = Integer.toString(i-_inputList.length+str.length());
                    }
                    }
                else {
                    idxList[i] = Integer.toString(i);
                    flag = 1;
                }     
            }
            int idx = 0;
            String result = "";
            int flag2 = 0;
            for (int i = 0; i < str.length(); i++) {
                try {
                    if (Integer.toString(i).equals(idxList[idx])) {
                        if (_inputList[idx].equals("1") || _inputList[idx].equals("1-")) {
                            result += str.substring(i, i+1);
                            flag2 = 1;
                        }
                        else {
                            result += mark;
                            flag2 = 0;
                        }
                        idx++;
                    }
                    else {
                        if (flag2==1) {
                            result += str.substring(i, i+1);
                        }
                        else { result += mark; }
                    }
                } catch (ArrayIndexOutOfBoundsException e) {
                    if (flag2==1) { result += str.substring(i, i+1); }
                    else { result += mark; }
                }
            }
            return result;

        }

        public String TopBottom(String str, double mean, double sd3){
            double lower = mean - sd3;
            double upper = mean + sd3;
            if(Double.parseDouble(str)< lower || Double.parseDouble(str) > upper){
                return Double.toString(mean);
            }
            else{
                return str;
            }
        }

        public String Rounding(String str, String option, int option2){

            Double data = Double.valueOf(str);  
            Double round_result;

            if (option2 > 0){
                if(option.equals("R")){  
                    round_result = Math.round(data*(Math.pow(10,(option2-1))))/(Math.pow(10,(option2-1)));
                } else if(option.equals("RU")){
                    round_result = Math.ceil(data*(Math.pow(10,(option2-1))))/(Math.pow(10,(option2-1)));
                } else {
                    round_result = Math.floor(data*(Math.pow(10,(option2-1))))/(Math.pow(10,(option2-1)));
                }
            } else {  
                if(option.equals("R")){  
                    round_result = Math.round(data/(Math.pow(10,(-1*option2)))) * (Math.pow(10,(-1*option2)));
                } else if(option.equals("RU")){
                    round_result = Math.ceil(data/(Math.pow(10,(-1*option2)))) * (Math.pow(10,(-1*option2)));
                } else {
                    round_result = Math.floor(data/(Math.pow(10,(-1*option2)))) * (Math.pow(10,(-1*option2)));
                }

            }
            return String.valueOf(round_result);
        }

        public String Randomize(String str, double mean, double sd3){

            String stringSet = "ㄱㄴㄷㄹㅁㅂㅅㅇㅈㅊㅋㅌㅍㅎ0123456789";

            try {
                double numValue = Double.parseDouble(str);
                Random rand = new Random();
                double randomNum = mean + (sd3 - mean) * rand.nextDouble();
            
                return Double.toString(randomNum);

            } catch (NumberFormatException e) {
                String outputString = "";
                int inputSize = str.length();
                int setSize = stringSet.length();

                Random rand = new Random();

                for (int i = 0; i < inputSize; i++) {
                    int randomIndex = rand.nextInt(setSize);
                    outputString += stringSet.charAt(randomIndex);
                }
                return outputString;
            }
        }


        public String Encryption(String str){ 
            try {
                MessageDigest md = MessageDigest.getInstance("SHA-256");
                
                byte[] hash = md.digest(str.getBytes(StandardCharsets.UTF_8));
                StringBuilder hexString = new StringBuilder();

                for (int i = 0; i < hash.length; i++) {
                    String hex = Integer.toHexString(0xff & hash[i]);
                    if(hex.length() == 1) hexString.append('0');
                    hexString.append(hex);
                }

                return hexString.toString();
            } catch(NoSuchAlgorithmException e){
                throw new RuntimeException(e);
            }
        }
    }

}



