import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxTuple implements Writable {
    Float min_date,max_date;
    String min_video_id,max_video_id;
    public void setMax_date(Float max_date) {
        this.max_date = max_date;
    }

    public void setMax_video_id(String max_video_id) {
        this.max_video_id = max_video_id;
    }

    public void setMin_date(Float min_date) {
        this.min_date = min_date;
    }

    public void setMin_video_id(String min_video_id) {
        this.min_video_id = min_video_id;
    }

    public Float getMax_date() {
        return max_date;
    }

    public String getMax_video_id() {
        return max_video_id;
    }

    public Float getMin_date() {
        return min_date;
    }

    public String getMin_video_id() {
        return min_video_id;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
       dataOutput.writeFloat(min_date);
       dataOutput.writeFloat(max_date);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        min_date = dataInput.readFloat();
        max_date = dataInput.readFloat();
    }

    @Override
    public String toString() {
        return  "Min Rated Video'" + min_video_id + '\t' +
                "Max Rated Video'" + max_video_id + '\t' +
                '\n' +
                min_date + '\t' +
                max_date + '\t'
                ;
    }
}
