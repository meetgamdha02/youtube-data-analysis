import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MinMaxComment {
    public static class Map extends Mapper<Object , Text , Text , MinMaxTouple>{
        MinMaxTouple minMaxTouple = new MinMaxTouple();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String temp = value.toString();
            String[] data = temp.split("\t");
            if(data.length>5){
                minMaxTouple.setMin_comment(Integer.parseInt(data[8]));
                minMaxTouple.setMax_comment(Integer.parseInt(data[8]));
                context.write(new Text(data[0]) , minMaxTouple);
            }
        }
    }

    public static class Reduce extends Reducer<Text , MinMaxTouple , Text , MinMaxTouple>{
        MinMaxTouple result;
        @Override
        protected void setup(Context context) {
            result = new MinMaxTouple();
            result.setMax_comment(Integer.MIN_VALUE);
            result.setMin_comment(Integer.MAX_VALUE);
        }

        @Override
        protected void reduce(Text key, Iterable<MinMaxTouple> values, Context context)  {
            for (MinMaxTouple minMaxTouple : values){
                if(minMaxTouple.getMax_comment() > result.getMax_comment()){
                    result.setMax_id(key.toString());
                    result.setMax_comment(minMaxTouple.getMax_comment());
                }
                if(minMaxTouple.getMin_comment() < result.getMin_comment()){
                    result.setMin_id(key.toString());
                    result.setMin_comment(minMaxTouple.getMin_comment());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("") , result);
        }
    }

    public static void main(String[] args) throws  Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration , "Comment");
        job.setJarByClass(MinMaxComment.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxTouple.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        FileInputFormat.addInputPath(job , new Path(args[0]));
        FileOutputFormat.setOutputPath(job , new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
