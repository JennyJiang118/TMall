package jcn;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import com.google.re2j.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.tools.GetConf;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.SequenceFileInputFilter;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.InverseMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.I;
import org.apache.zookeeper.txn.Txn;

import sun.tools.asm.CatchData;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;




public class YoungSell {

    public static class TokenizerMapper extends Mapper<Object,  Text , Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        //private Text product = new Text();
        private Set<String> youngUsers = new HashSet<>();
        private BufferedReader fis;
        private Configuration conf;

        private void parseUsersFile(String fileName){
            try{
                fis = new BufferedReader(new FileReader(fileName));
                String line = null;
                while((line = fis.readLine())!=null){ 
                            
                    String[] userInfo = line.split(",");
                    if(userInfo.length == 3){
                        String user = userInfo[0];
                        String age = userInfo[1];

                    if(age.equals("1")||age.equals("2")||age.equals("3")){
                        youngUsers.add(user);
                    }

                    }
                    
                }

                }catch(IOException ioe){
                    System.err.println("Caught exception while parsing the cached file'"+
                    StringUtils.stringifyException(ioe));
            }
        }

        public void setup(Context context) throws IOException,InterruptedException{
            this.conf = context.getConfiguration();
            URI[] patternsURIs = Job.getInstance(this.conf).getCacheFiles();
            URI patternURI = patternsURIs[0];
            Path patternPath = new Path(patternURI.getPath());
            String patternFileName = patternPath.getName().toString();
            parseUsersFile(patternFileName);
        }


        //@Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String line = value.toString();
            String[] data = line.split(",");
            String date = data[5];
            String action = data[6];
            String seller = data[3];
            String user = data[0];

            if(youngUsers.contains(user)==true && date.equals("1111")){
                if(action.equals("1")||action.equals("2")||action.equals("3")){
                    context.write(new Text(seller), one);
                }
            }     
                          
        }
    }



    private static class IntWritableDecreasingComparator extends IntWritable.Comparator{
        public int compare(WritableComparable a, WritableComparable b){
            return -super.compare(a, b);
        }

        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class InverseMapper<K,V> extends Mapper<K, V, V, K>{
        @Override
        public void map(K key, V value, Context context) throws IOException, InterruptedException{
            context.write(value, key);
        }
    }

    public static class SortReducer extends Reducer<IntWritable, Text, Text, NullWritable>{
        private Text result = new Text();
        int i =0;
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable,Text,Text,NullWritable>.Context context) throws IOException,InterruptedException{
            for (Text val:values){
                if(i>=100) break;
                i++;
                result.set(val.toString());
                String str = "排名"+i+":"+result+",共计"+key+"次";
                context.write(new Text(str), NullWritable.get());
            }

        }
    }



    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{
            int sum = 0;
            for(IntWritable val: values){
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 3){
            System.err.println("err");
            System.exit(2);
        }


        //1 
        Path tempDir = new Path("hotsell-tmp-output");


        //Job job = new Job(conf, "best seller");
        Job job = Job.getInstance(conf, "best seller");
        job.setJarByClass(HotSell.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);//

        job.addCacheFile((new Path(otherArgs[1])).toUri());

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, tempDir);


        //2
        job.waitForCompletion(true);

        Job sortjob = Job.getInstance(conf, "sort");
        FileInputFormat.addInputPath(sortjob, tempDir);
        sortjob.setJarByClass(HotSell.class);
        sortjob.setInputFormatClass(SequenceFileInputFormat.class);
        sortjob.setMapperClass(InverseMapper.class);
        sortjob.setNumReduceTasks(1);
        sortjob.setReducerClass(SortReducer.class);
        FileOutputFormat.setOutputPath(sortjob, new Path(otherArgs[2]));
        sortjob.setOutputKeyClass(IntWritable.class);
        sortjob.setOutputValueClass(Text.class);
        sortjob.setSortComparatorClass(IntWritableDecreasingComparator.class);

        sortjob.waitForCompletion(true);

        FileSystem.get(conf).delete(tempDir);
        System.exit(0);

    }
}