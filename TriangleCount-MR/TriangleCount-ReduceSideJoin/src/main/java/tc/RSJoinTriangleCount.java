package tc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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



public class RSJoinTriangleCount extends Configured implements Tool {

    public enum COUNTER {
        TriangleCount
    };

    private static final Logger logger = LogManager.getLogger(RSJoinTriangleCount.class);

    @Override
    public int run(final String[] args) throws Exception {
        JobControl jobControl = new JobControl("jobChain");
        final Configuration conf1 = getConf();
        String[] otherArgs = new GenericOptionsParser(conf1, args)
                .getRemainingArgs();
        String MaxFilter = otherArgs[2];
        final Job job1 = Job.getInstance(conf1, "2-path Count");
        job1.setJarByClass(RSJoinTriangleCount.class);
        final Configuration jobConf = job1.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");
//      =======================================================================
		 //Delete output directory, only to ease local development; will not work on AWS.
//        final FileSystem fileSystem = FileSystem.get(conf1);
//        if (fileSystem.exists(new Path(args[1]))) {
//            fileSystem.delete(new Path(args[1]), true);
//        }
//		 ======================================================================
        job1.getConfiguration().set("max.filter", MaxFilter);
        job1.setMapperClass(TwoPathMapper.class);
        job1.setReducerClass(TwoPathReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/temp"));

        int finished1st = job1.waitForCompletion(true) ? 0:1;

        if (finished1st==0) {
            Configuration conf2 = getConf();

            Job job2 = Job.getInstance(conf2);
            job2.setJarByClass(RSJoinTriangleCount.class);
            job2.setJobName("Triangle Count");
            job2.getConfiguration().set("max.filter", MaxFilter);

            MultipleInputs.addInputPath(job2, new Path(args[1] + "/temp"),
                    TextInputFormat.class, RSMappers.TriangleMapper2.class);

            MultipleInputs.addInputPath(job2, new Path(otherArgs[0]),
                    TextInputFormat.class, RSMappers.TriangleMapper1.class);
            FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));

            job2.setReducerClass(TriangleReducer.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            int finished = job2.waitForCompletion(true)? 0:1;
            Counters cn=job2.getCounters();
            Counter c1=cn.findCounter(COUNTER.TriangleCount);
            logger.info(c1.getDisplayName()+":"+ c1.getValue()/3);
            return finished;
        }
        return finished1st;
    }

    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new Error("Three arguments required:\n<input-dir> <output-dir> <MaxNodeNumber>");
        }
        try {
            ToolRunner.run(new RSJoinTriangleCount(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}