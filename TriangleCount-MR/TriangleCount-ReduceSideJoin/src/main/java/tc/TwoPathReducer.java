package tc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class TwoPathReducer extends Reducer<Text, Text, Text, Text> {
    private final IntWritable result = new IntWritable();
    private ArrayList<Text> listI = new ArrayList<Text>();
    private ArrayList<Text> listO = new ArrayList<Text>();

    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        // Clear our lists
        listI.clear();
        listO.clear();

        for (final Text val : values) {
            if (val.charAt(0) == 'I') {
                listI.add(new Text(val.toString().substring(1)));
            }
            else if (val.charAt(0) == 'O'){
                listO.add(new Text(val.toString().substring(1)));

            }
        }

        executeJoinLogic(context);
    }
    private void executeJoinLogic(Context context) throws IOException, InterruptedException {
        if (!listI.isEmpty() && !listO.isEmpty()) {
            for (Text i : listI) {
                for (Text j : listO) {
                    if (!i.equals(j)) {
                        context.write(i, j);
                    }
                }
            }
        }
    }

}
