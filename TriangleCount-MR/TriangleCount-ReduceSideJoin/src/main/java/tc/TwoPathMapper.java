package tc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class TwoPathMapper extends Mapper<Object, Text, Text, Text> {
    private static final String I = "I";
    private static final String O = "O";

    private final Text E1 = new Text();
    private final Text E2 = new Text();
    private final Text EdgeVal = new Text();
    private int MAX;

    @Override
    public void setup(Context context) {
        // Get the Max val passed to context by user from our configuration
        MAX = Integer.parseInt(context.getConfiguration().get("max.filter"));
        if (MAX == -1) {
            MAX = 11316811;
        }
    }
    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        // each map call gets one line i.e. one edge to process.
        // splitting the incoming edge represented by a line into nodes
        final String[] edge = value.toString().split(",");
        // Filter to disregard nodes above the max filter val
        if (Integer.parseInt(edge[0]) <= MAX & Integer.parseInt(edge[1]) <= MAX) {
            E1.set(edge[0]);
            E2.set(edge[1]);
            EdgeVal.set(O+edge[0]);
            context.write(E2,EdgeVal);
            EdgeVal.set(I+edge[1]);
            context.write(E1,EdgeVal);
        }
    }
}

