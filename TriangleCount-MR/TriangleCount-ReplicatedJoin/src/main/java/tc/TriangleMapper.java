package tc;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;


public class TriangleMapper extends Mapper<Object, Text, Text, Text> {
    private static final Text I = new Text("I");
    private static final Text O = new Text("O");

    private final Text E1 = new Text();
    private final Text E2 = new Text();
    private int MAX;


    private HashMap<Integer, ArrayList<Integer>> cachedEdges = new HashMap<Integer, ArrayList<Integer>>();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
        // Get the type of max val from our configuration
        MAX = Integer.parseInt(context.getConfiguration().get("max.filter"));
        if (MAX == -1) {
            MAX = 11316811;
        }

        URI[] cacheFiles = context.getCacheFiles();
        if (cacheFiles != null && cacheFiles.length > 0) {
            try
            {
                BufferedReader reader = new BufferedReader(new FileReader("CacheFILE"));
                String in;


                // Reading in the cache file into a hashmap

                while ((in = reader.readLine()) != null){
                    final String[] line = in.toString().split(",");
                    int n1 = Integer.parseInt(line[0]);
                    int n2 = Integer.parseInt(line[1]);
                    if (n1 <= MAX & n2 <= MAX) {
                        if (cachedEdges.get(n1) == null) {                  //gets the value for an id
                            cachedEdges.put(n1, new ArrayList<Integer>()); //no ArrayList assigned, create new ArrayList
                            cachedEdges.get(n1).add(n2);                    //adds value to list.
                        }
                        else{
                            cachedEdges.get(n1).add(n2); //else just adds value to list.
                        }

                    }
                }
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        // each map call gets one line i.e. one edge to process.
        // splitting the incoming edge represented by a line into nodes
        final String[] line = value.toString().split(",");
        final int[] edge = new int[]{Integer.parseInt(line[0]),Integer.parseInt(line[1])};
        // Filter to disregard nodes above the max filter val
        if (edge[0] <= MAX & edge[1] <= MAX) {
            if (cachedEdges.containsKey(edge[1])) {                     // check if node 2 is in hashmap
                for (int val : cachedEdges.get(edge[1])) {              // get the list of 2paths for node 2
                    if (cachedEdges.containsKey(val)) {                 // if node 1 is present in the list foreach
                        if (cachedEdges.get(val).contains(edge[0])) {   //value in as key then triangle is found
                            context.getCounter(RepJoinTriangleCount.COUNTER.TriangleCount).increment(1);
                        }
                    }
                }
            }
        }
    }
}

