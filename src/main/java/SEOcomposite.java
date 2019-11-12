import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import javax.annotation.Nonnull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SEOcomposite implements WritableComparable<SEOcomposite> {
    private Text host;
    private IntWritable num;

    SEOcomposite() {
        this.host = new Text("");
        this.num = new IntWritable(0);
    }

    SEOcomposite(String host, int num) {
        this.host = new Text(host);
        this.num = new IntWritable(num);
    }

    @Override
    public int compareTo(@Nonnull SEOcomposite o) {
        int cmp = this.host.toString().compareTo(o.getHost().toString());

        if(cmp == 0) {
            return Integer.compare(this.num.get(), o.getNum().get());
        }
        else {
            return cmp;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.host.write(out);
        this.num.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.host.readFields(in);
        this.num.readFields(in);
    }

    @Override
    public int hashCode() {
        return host.hashCode() * 179 + num.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SEOcomposite) {
            SEOcomposite tmp = (SEOcomposite) obj;
            return (host.equals(tmp.host)) && (num.get() == tmp.num.get());
        }
        return false;
    }

    @Override
    public String toString() {
        return host + "\t" + String.valueOf(num);
    }

    public Text getHost() {
        return this.host;
    }

    public void setHost(Text host) {
        this.host = host;
    }

    public IntWritable getNum() {
        return this.num;
    }

    public void setNum(IntWritable num) {
        this.num = num;
    }
}
