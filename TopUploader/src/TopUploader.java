import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

public class TopUploader {
    public static class Map extends Mapper<LongWritable , Text , Text , IntWritable>{
        public void map(LongWritable key , Text val , Context context) throws IOException , InterruptedException{
            String value = val.toString();
            String[] data = value.split("\t");
            Text uploader = new Text();
            if(data.length>2){
                uploader.set(data[1]);
                context.write(uploader , new IntWritable(1));
            }
        }
    }

    public static class Reduce extends Reducer<Text , IntWritable , Text , IntWritable>{
        TreeMap<Integer , String>treeMap;
        public void setup(Context context){
            treeMap = new TreeMap<>();
        }
        public void reduce(Text key , Iterable<IntWritable>value , Context context) throws IOException,InterruptedException{
            int sum = 0;
            for(IntWritable values : value)sum+= values.get();
            treeMap.put(sum , key.toString());
            if(treeMap.size()>10)treeMap.remove(treeMap.firstKey());
        }

        public void cleanup(Context context) throws IOException , InterruptedException{
            for(java.util.Map.Entry<Integer , String>temp : treeMap.entrySet()){
                context.write(new Text(temp.getValue()) , new IntWritable(temp.getKey()));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration , "TopUploader");
        job.setJarByClass(TopUploader.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        FileInputFormat.addInputPath(job , new Path(args[0]));
        FileOutputFormat.setOutputPath(job , new Path(args[1]));
        System.exit(job.waitForCompletion(true) ?0 : 1);
    }
}
