import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MinMaxTouple implements Writable {
    int max_comment , min_comment;
    String max_id , min_id;

    public int getMax_comment() {
        return max_comment;
    }

    public int getMin_comment() {
        return min_comment;
    }

    public String getMax_id() {
        return max_id;
    }

    public String getMin_id() {
        return min_id;
    }

    public void setMax_comment(int max_comment) {
        this.max_comment = max_comment;
    }

    public void setMax_id(String max_id) {
        this.max_id = max_id;
    }

    public void setMin_comment(int min_comment) {
        this.min_comment = min_comment;
    }

    public void setMin_id(String min_id) {
        this.min_id = min_id;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(min_comment);
        dataOutput.writeInt(max_comment);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        min_comment = dataInput.readInt();
        max_comment = dataInput.readInt();
    }

    @Override
    public String toString() {
        return
                "Max.Number of Comment=" + max_comment +
                ", ID='" + max_id + '\'' +
                ", Min Number Of Comment=" + min_comment +

                ", ID='" + min_id + '\'' +
                '}';
    }
}
