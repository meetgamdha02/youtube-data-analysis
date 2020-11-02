import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.TreeMap;

public class TopRated {
    public static class Map extends Mapper<LongWritable , Text , Text , FloatWritable>{
        public void map(LongWritable key , Text value , Context context) throws IOException , InterruptedException {
            String val = value.toString();
            String[] data = val.split("\t");
            FloatWritable ratings = new FloatWritable();
            if(data.length>9){
                ratings.set(Float.parseFloat(data[6]));
            }
            context.write(new Text(data[0]) , ratings);
        }
    }

    public static class Reduce extends Reducer<Text , FloatWritable , Text , FloatWritable>{
        TreeMap<Float , String>treeMap;
        public void setup(Context context){
            treeMap = new TreeMap<>();
        }
        public void reduce(Text key , Iterable<FloatWritable>value , Context context){
            float ratings = 0;
            for (FloatWritable values : value){
                ratings = values.get();
            }
            treeMap.put(ratings , key.toString());
            if(treeMap.size()>10)treeMap.remove(treeMap.firstKey());
        }
        public void cleanup(Context context) throws  IOException , InterruptedException{
            for (java.util.Map.Entry<Float , String>temp : treeMap.entrySet()){
                context.write(new Text(temp.getValue()) , new FloatWritable(temp.getKey()));
            }
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration , "TopRated");
        job.setJarByClass(TopRated.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        FileInputFormat.addInputPath(job , new Path(args[0]));
        FileOutputFormat.setOutputPath(job , new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
