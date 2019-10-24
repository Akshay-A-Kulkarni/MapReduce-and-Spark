package tc;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class RSMappers{

    public static class TriangleMapper1 extends Mapper<Object, Text, Text, Text> {

        private final Text EDGE = new Text();
        private final Text FILE = new Text();


        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException,
                InterruptedException {
            // Set the incoming edge as the key since we want to compare the whole record with the other file

            EDGE.set(value);
            FILE.set("A");
            context.write(EDGE, FILE);
        }
    }

    public static class TriangleMapper2 extends Mapper<Object, Text, Text, Text> {
        private final Text EDGE = new Text();
        private final Text FILE = new Text();


        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException,
                InterruptedException {
        // Set the incoming edge as the key since we want to compare the whole record with the other file
            EDGE.set(value);
            FILE.set("B");
            context.write(EDGE, FILE);

        }
    }
}

