import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class SEOcomposite implements WritableComparable<SEOcomposite> {
    private Text url;
    private Text query;

    public SEOcomposite() {
        this.url = new Text("");
        this.query = new Text("");
    }

    SEOcomposite(String url, String query) {
        this.url = new Text(url);
        this.query = new Text(query);
    }

    @Override
    public int compareTo(SEOcomposite o) {
        int cmp = this.url.toString().compareTo(o.getUrl().toString());

        if(cmp == 0) {
            return query.toString().compareTo(o.getQuery().toString());
        }
        else {
            return cmp;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        url.write(out);
        query.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        url.readFields(in);
        query.readFields(in);
    }

    @Override
    public int hashCode() {
        return url.hashCode() * 163 + query.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SEOcomposite) {
            SEOcomposite p = (SEOcomposite) obj;
            return url.equals(p.url) && query.equals(p.query);
        }
        return false;
    }

    @Override
    public String toString() {
        return url + "\t" + query;
    }

    public Text getUrl() {
        return url;
    }

    public void setUrl(Text url) {
        this.url = url;
    }

    public Text getQuery() {
        return query;
    }

    public void setQuery(Text query) {
        this.query = query;
    }
}
