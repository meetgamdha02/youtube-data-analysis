import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MinMax {
    public static class Map extends Mapper<Object , Text , Text , MinMaxTuple>{
        MinMaxTuple minMaxTuple = new MinMaxTuple();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String temp = value.toString();
            String[]data = temp.split("\t");
            if(data.length>9){
                minMaxTuple.setMax_date(Float.parseFloat(data[6]));
                minMaxTuple.setMin_date(Float.parseFloat(data[6]));
                context.write(new Text(data[0]) , minMaxTuple);
            }
        }
    }
    public static class Reduce extends Reducer<Text , MinMaxTuple , Text , MinMaxTuple>{
        MinMaxTuple result;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            result = new MinMaxTuple();
            result.setMin_date(null);
            result.setMax_date(null);

        }

        @Override
        protected void reduce(Text key, Iterable<MinMaxTuple> values, Context context) throws IOException, InterruptedException {
            for(MinMaxTuple minMaxTuple : values){
                if(result.getMax_date() == null || minMaxTuple.getMax_date().compareTo(result.getMax_date()) > 0){
                    result.setMax_date(minMaxTuple.getMax_date());
                    result.setMax_video_id(key.toString());
                }
                if(result.getMin_date() == null || minMaxTuple.getMin_date().compareTo(result.getMin_date())<0){
                    result.setMin_date(minMaxTuple.getMin_date());
                    result.setMin_video_id(key.toString());
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("") , result);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration , "MinMax");
        job.setJarByClass(MinMax.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxTuple.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        FileInputFormat.addInputPath(job , new Path(args[0]));
        FileOutputFormat.setOutputPath(job , new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
