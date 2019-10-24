package tc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.net.URI;


public class RepJoinTriangleCount extends Configured implements Tool {

    public enum COUNTER {
        TriangleCount
    };

    private static final Logger logger = LogManager.getLogger(RepJoinTriangleCount.class);

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        String MaxFilter = otherArgs[2];
        final Job job = Job.getInstance(conf, "RepJoin");
        job.setJarByClass(RepJoinTriangleCount.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");

//      =======================================================================
		 //Delete output directory, only to ease local development; will not work on AWS.
//        final FileSystem fileSystem = FileSystem.get(conf);
//        if (fileSystem.exists(new Path(args[1]))) {
//            fileSystem.delete(new Path(args[1]), true);
//        }
//		 ======================================================================

        job.getConfiguration().set("max.filter", MaxFilter);
        job.setMapperClass(TriangleMapper.class);
        job.setNumReduceTasks(0);
        job.addCacheFile(new URI (args[0] + "#CacheFILE"));

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int finished = job.waitForCompletion(true) ? 0:1;

        Counters cn=job.getCounters();
        Counter c1=cn.findCounter(COUNTER.TriangleCount);
        logger.info(c1.getDisplayName()+":"+ c1.getValue()/3);

        return finished;
    }

    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new Error("Three arguments required:\n<input-dir> <output-dir> <MaxNodeNumber>");
        }
        try {
            ToolRunner.run(new RepJoinTriangleCount(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}