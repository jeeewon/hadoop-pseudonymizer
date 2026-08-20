
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.hadoop.fs.FSDataInputStream;

//ENCRYPTION 
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
import java.util.HashSet;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.DataInput;
import java.io.DataOutput;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;



public class Join {
    public static String Encryption(String str){ 
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

    public static class JoinMapper extends Mapper<Object, Text, Text, Text> {

        
        private List<Integer> joinKeyIndicesA = new ArrayList<>();
        private List<Integer> joinKeyIndicesB = new ArrayList<>();
        private List<Integer> columnIndicesA = new ArrayList<>();
        private List<Integer> columnIndicesB = new ArrayList<>();        
        private String fileTag = ""; 

        @Override
        protected void setup(Context context) throws IOException, InterruptedException { 

            Configuration conf = context.getConfiguration();

            joinKeyIndicesA = parseIndices(conf, "joinKeyIndicesA");
            joinKeyIndicesB = parseIndices(conf, "joinKeyIndicesB");
            columnIndicesA = parseIndices(conf, "columnIndicesA");
            columnIndicesB = parseIndices(conf, "columnIndicesB");

            Path filePath = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath();
            String foldName = filePath.toString();
            fileTag = determineFileTag(foldName); // 파일 태그를 초기화

        }

        private List<Integer> parseIndices(Configuration conf, String key) {
            List<Integer> indices = new ArrayList<>();
            String indexStr = conf.get(key);
            if (indexStr != null && !indexStr.isEmpty()) {
                for (String numStr : indexStr.split(",")) {
                    indices.add(Integer.parseInt(numStr.trim()));
                }
            }
            return indices;
        }

        private String determineFileTag(String filePath) {

            if (filePath.contains("/fileA/")) {
                return "fileA";
            } else if (filePath.contains("/fileB/")) {
                return "fileB";
            }
            return "";
        }


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();

            if (isHeader(line)) return;

            String[] values = line.split(",");

            String joinKey = createJoinKey(values, fileTag.equals("fileA") ? joinKeyIndicesA : joinKeyIndicesB);
            String encryptedJoinKey = Encryption(joinKey); // 암호화


            List<String> selectedColumns = selectColumns(values, fileTag.equals("fileA") ? columnIndicesA : columnIndicesB);
            
            String allColumns = String.join(",", selectedColumns);
            
            context.write(new Text(encryptedJoinKey), new Text(fileTag + "\t" + allColumns));

        }

        private boolean isHeader(String line) {
            return line.contains("Id");
        }



        private String createJoinKey(String[] values, List<Integer> joinKeyIndices) {
            StringBuilder joinKeyBuilder = new StringBuilder();
            for (int index : joinKeyIndices) {
                joinKeyBuilder.append(values[index]);
            }
            return joinKeyBuilder.toString();
        }

        private List<String> selectColumns(String[] values, List<Integer> columnIndices) {
            List<String> selectedColumns = new ArrayList<>();
            for (int index : columnIndices) {
                selectedColumns.add(values[index]);
            }
            return selectedColumns;
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, NullWritable> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> fileARecords = new ArrayList<>();
            List<String> fileBRecords = new ArrayList<>();

            for (Text value : values) {
                String[] parts = value.toString().split("\t", 2); // Split into tag and record
                String tag = parts[0];
                String record = parts[1];

                if (tag.equals("fileA")) {
                    fileARecords.add(record);
                } else if (tag.equals("fileB")) {
                    fileBRecords.add(record);
                }
            }

            for (String fileARecord : fileARecords) {
                for (String fileBRecord : fileBRecords) {
                    context.write(new Text(fileARecord + "," + fileBRecord), NullWritable.get());
                }
            }
        }

    }


    private static List<String> getColumnIndexList(String columns, String jsonFilePath, FileSystem hdfs, String tableTag, String keyType) throws IOException, ParseException {
    List<String> columnIndexList = new ArrayList<>();
    Path jsonPath = new Path(jsonFilePath);
    JSONParser parser = new JSONParser();

    try (InputStream is = hdfs.open(jsonPath);
         InputStreamReader isr = new InputStreamReader(is, StandardCharsets.UTF_8)) {
        JSONObject jsonObject = (JSONObject) parser.parse(isr);
        JSONObject jsonColumnObject = (JSONObject) jsonObject.get(keyType);
        JSONArray jsonColumnArray = (JSONArray) jsonColumnObject.get(tableTag);

        List<String> columnNames = Arrays.asList(columns.split(","));

        for (Object column : jsonColumnArray) {
            String columnName = (String) column;
            int index = columnNames.indexOf(columnName);
            if (index != -1) {
                columnIndexList.add(String.valueOf(index));
            }
        }
    }
    return columnIndexList;
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

    private static void setupConfiguration(Configuration conf) {
            

        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapred.output.compression.type", "BLOCK");
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

        // 설정 값 지정
        conf.set("yarn.app.mapreduce.am.resource.mb", "8192");
        conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx6144m");
        conf.set("mapreduce.map.memory.mb", "8192");
        conf.set("mapreduce.map.java.opts", "-Xmx6144m");
        conf.set("mapreduce.map.cpu.vcores", "4");
        conf.set("mapreduce.reduce.memory.mb", "16384");
        conf.set("mapreduce.reduce.java.opts", "-Xmx12288m");
        conf.set("mapreduce.reduce.cpu.vcores", "4");

        // 추가적인 성능 최적화 설정
        conf.set("mapreduce.task.io.sort.mb", "2047"); // 2GB 이하로 설정
        conf.set("mapreduce.map.sort.spill.percent", "0.8"); // 80%에서 스필
        conf.set("mapreduce.task.io.sort.factor", "100"); // 100개의 파일 병합
        conf.set("mapreduce.job.jvm.numtasks", "-1"); // JVM 재사용


        // 셔플 및 리듀스 단계 최적화 설정
        conf.set("mapreduce.reduce.shuffle.parallelcopies", "50"); // 셔플 스레드 수
        conf.set("mapreduce.reduce.memory.total.bytes", "8388608"); // 8GB 메모리 버퍼 (8192MB * 1024)
        conf.set("mapreduce.reduce.shuffle.input.buffer.percent", "0.7"); // 70% 메모리 버퍼 임계값
        conf.set("mapreduce.reduce.shuffle.memory.limit.percent", "0.25"); // 25% 파일 임계값

        // 매퍼 및 리듀서 개수 설정
        conf.set("mapreduce.input.fileinputformat.split.maxsize", String.valueOf(256 * 1024 * 1024));
        conf.set("mapreduce.input.fileinputformat.split.minsize", String.valueOf(256 * 1024 * 1024));
        conf.set("mapreduce.job.maps", "8192");
        conf.set("mapreduce.job.reduces", "2048");


    }
    
    private static void printExecutionTime(long startTime) {
        long endTime = System.currentTimeMillis(); // 작업 종료 시간 기록
        long elapsedTime = endTime - startTime;
        long elapsedMinutes = elapsedTime / 60000; // 밀리초를 분으로 변환
        long remainingSeconds = (elapsedTime % 60000) / 1000; // 남은 밀리초를 초로 변환
        System.out.println("작업 실행 시간: " + elapsedTime + " ms (" + elapsedMinutes + " 분 " + remainingSeconds + " 초)"); // 실행 시간 출력
    }

    public static void main(String[] args) throws Exception {
	    long startTime = System.currentTimeMillis(); //작업시간 기록
        System.out.println("Join start!");

        if (args.length != 4) {
            System.err.println("Usage: BigProtector <input path A> <input path B> <json path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        setupConfiguration(conf);

        String inputPathA = args[0];
        String inputPathB = args[1];
        String jsonPath   = args[2];
        String outputPath = args[3];

        // 컬럼명 추출 
        String columnsA = readFirstLineFromFirstFile(fs, inputPathA);
        String columnsB = readFirstLineFromFirstFile(fs, inputPathB);

        // 컬럼명을 Configuration에 설정
        conf.set("columnsA", columnsA);
        conf.set("columnsB", columnsB);

        //join_key 받아오기 
        List<String> joinKeyIndicesA = getColumnIndexList(columnsA, jsonPath, fs, "A", "join_key");
        List<String> joinKeyIndicesB = getColumnIndexList(columnsB, jsonPath, fs, "B", "join_key");
        conf.set("joinKeyIndicesA", String.join(",", joinKeyIndicesA));
        conf.set("joinKeyIndicesB", String.join(",", joinKeyIndicesB));

        //column_list받아오기 
        List<String> columnIndicesA = getColumnIndexList(columnsA, jsonPath, fs, "A", "column_list");
        List<String> columnIndicesB = getColumnIndexList(columnsB, jsonPath, fs, "B", "column_list");
        conf.set("columnIndicesA", String.join(",", columnIndicesA));
        conf.set("columnIndicesB", String.join(",", columnIndicesB));

   
        Job job = Job.getInstance(conf, "Join");
        job.addCacheFile(new URI(args[2] + "#Benchmark.json"));
        job.setJarByClass(App.class);
        job.setMapperClass(JoinMapper.class);
        //job.setNumReduceTasks(0);
        job.setReducerClass(JoinReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);


        FileInputFormat.addInputPath(job, new Path(inputPathA));
        FileInputFormat.addInputPath(job, new Path(inputPathB));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        if(!job.waitForCompletion(true)){
            throw new RuntimeException("join failed");
        };
        
        System.out.println("---------------------------------------------");
        System.out.println("수행 기능: inner join");
        System.out.println("테이블 A: "+inputPathA);
        System.out.println("테이블 B: "+inputPathB);
        printExecutionTime(startTime);
        System.out.println("hdfs 저장완료: "+outputPath);
    }
}






