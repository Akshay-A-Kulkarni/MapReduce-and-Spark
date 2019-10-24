package tc;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class TriangleReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException {

        long countA = 0;
        long countB = 0;

        for (final Text val : values) {
            if (val.charAt(0) == 'A') {
                countA++;
            }
            else if (val.charAt(0) == 'B'){
                countB++;
            }
        }

        if (countA>0){
            context.getCounter(RSJoinTriangleCount.COUNTER.TriangleCount).increment(countB);

        }
    }

}