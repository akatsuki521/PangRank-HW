import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.mapreduce.Cluster;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapred.Counters.Counter;


public class PageRank{
    static enum PageCount{
        Count, TotalPR, NodeCount, MaxDegree, MinDegree, Edges, TopTen, MaxPr, Timer
    }
    public static class RankKey implements WritableComparable<RankKey>{
        public Text node;
        public FloatWritable pr;

        public RankKey(){
            node=new Text("");
            pr=new FloatWritable();
        }
        public RankKey(Text node, FloatWritable pr){
            this.node=node;
            this.pr=pr;
        }
        @Override
        public int compareTo(RankKey k){
            return -pr.compareTo(k.pr);
        }//Since result will be sorted by key, so in this way to get the top 10 ranking.

        @Override
        public void write(DataOutput out) throws IOException{
            node.write(out);
            pr.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException{
            node.readFields(in);
            pr.readFields(in);
        }

        @Override
        public int hashCode(){
            return node.toString().hashCode();
        }
    }

    public static class SortMapper extends Mapper<Object, Text, RankKey, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            if(value.toString().trim().isEmpty())
                return;
            String[] array=value.toString().split("\\s+");
            int index=array[1].indexOf(".");
            if(index!=-1){
                context.write(new RankKey(new Text(array[0]), new FloatWritable(Float.valueOf(array[1]))), new Text(""));
            }//Automatic rank.


        }
    }

    public static class SortReducer extends Reducer<RankKey, Text, Text, Text>{

        public void reduce(RankKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            long count=context.getCounter(PageCount.TopTen).getValue();
            if(count<10){
                context.write(key.node, new Text(String.valueOf(key.pr)));
            }
            context.getCounter(PageCount.TopTen).increment(1);
        }
    }






    public static class PageRankMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            context.getCounter(PageCount.Count).increment(1);
            String tuple=value.toString();
            String[] array=tuple.trim().split("\\s+");
            if(array[0].trim().isEmpty())
                return;
            int len=array.length;
            float score=1.0f;
            int flag=0;//If array[1] is a float, flag=1 is a offset.
            if(len!=1&&isFloat(array[1])){//If has a pr value.
                score=Float.valueOf(array[1]);
                flag=1;
            }else{//If first seen.
                context.getCounter(PageCount.NodeCount).increment(1);
                long curMax=context.getCounter(PageCount.MaxDegree).getValue();
                long curMin=context.getCounter(PageCount.MinDegree).getValue();
                if((int)curMax<len-1)
                    context.getCounter(PageCount.MaxDegree).setValue(len-1);
                if((int)curMin>len-1)
                    context.getCounter(PageCount.MinDegree).setValue(len-1);
                context.getCounter(PageCount.Edges).increment(len-1);
            }
            if(len==flag+1){//Node without out link.
                context.write(new Text(array[0]), new Text(String.valueOf(score)));
            }else{
                StringBuffer outLink=new StringBuffer();
                outLink.append(array[1+flag]);
                for(int i=0; i<len-2-flag; i++){
                    outLink.append(" ");
                    outLink.append(array[i+2+flag]);
                }
                context.write(new Text(array[0]), new Text(outLink.toString()));
                score=score/(len-1-flag)*1.0f;
                for(int i=0; i<len-1-flag; i++){
                    context.write(new Text(array[i+1+flag]), new Text(String.valueOf(score)));
                }
            }

        }

        private boolean isFloat(String s){
            int index=s.indexOf(".");
            if(index==-1)
                return false;
            return true;
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text>{

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            StringBuffer outLink=new StringBuffer();
            float factor=0.85f;
            float pr=0f;
            for(Text f: values){
                String value=f.toString();
                int s=value.indexOf(".");
                if(s!=-1){
                    pr+=Float.valueOf(value);
                } else {
                    outLink.append(f.toString());
                }
            }

            pr=(1-factor)+factor*pr;
            context.getCounter(PageCount.TotalPR).increment((int)(pr*1000));
            String output=pr+" "+outLink.toString();
            context.write(key, new Text(output));
        }
    }

    public static void main(String[] args) throws Exception{
        String input;
        String output;
        String time = "Time for first ten cycle: ";
        float threshold = Float.valueOf(args[2]);
        int iteration = 0;
        int iterationLimit = 50;//Max iterations.
        boolean status = false;
        Path inputPath = new Path(args[0]);
        Path upper = inputPath.getParent();
        input = args[0];
        output = args[1];
        String infoPath = args[0] + "static/info";
        String cyclePath= args[0] + "static/cycle";
        String rankResult = args[0] + "static/rank";
        String timeResult = args[0] + "static/time";
        long prSum = 0;
        long startTime = System.currentTimeMillis();
        long endTime = 0;

        while (iteration < iterationLimit){

            if (iteration > 0){
                input=output;
                output=args[1]+iteration;
            }

            Configuration conf = new Configuration();
            FileSystem fs = upper.getFileSystem(conf);
            //fs.delete(new Path(output), true);

            Job job = Job.getInstance(conf, "pagerank");
            job.setJarByClass(PageRank.class);
            job.setMapperClass(PageRankMapper.class);
            job.setReducerClass(PageRankReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            TextInputFormat.addInputPath(job, new Path(input));
            TextOutputFormat.setOutputPath(job, new Path(output));
            status = job.waitForCompletion(true);
            iteration++;

            long totalPr = job.getCounters().findCounter(PageCount.TotalPR).getValue();

            if (iteration==1){
                fs.delete(new Path(infoPath), true);
                FSDataOutputStream outStream = fs.create(new Path(infoPath));
                StringBuffer resultStringBuffer = new StringBuffer();
                resultStringBuffer.append("Nodes Num: ");
                resultStringBuffer.append(job.getCounters().findCounter(PageCount.NodeCount).getValue());
                resultStringBuffer.append("\nMax degree: ");
                resultStringBuffer.append(job.getCounters().findCounter(PageCount.MaxDegree).getValue());
                resultStringBuffer.append("\nMin degree: ");
                resultStringBuffer.append(job.getCounters().findCounter(PageCount.MinDegree).getValue());
                resultStringBuffer.append("\nEdges: ");
                resultStringBuffer.append(job.getCounters().findCounter(PageCount.Edges).getValue());
                String resString = resultStringBuffer.toString();
                outStream.write(resString.getBytes());
                outStream.close();
            }

            if (iteration == 1){
                endTime = startTime - System.currentTimeMillis();
                time = time + endTime + "\n";
            }


            if (Math.abs(prSum - totalPr) < 1000*threshold && iteration > 11){//If the diff of sum of pageranks less than threshold, break. At least 11 iters.
                fs.delete(new Path(cyclePath), true);
                FSDataOutputStream outStream = fs.create(new Path(cyclePath));
                String resString = "Finishing Iteration: "+iteration;
                outStream.write(resString.getBytes());
                outStream.close();
                break;
            }
            prSum = totalPr;
        }
        Configuration conf = new Configuration();
        FileSystem fs = upper.getFileSystem(conf);
        fs.delete(new Path(rankResult), true);
        Job job = Job.getInstance(conf, "sort");
        job.setJarByClass(PageRank.class);
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        job.setNumReduceTasks(1);//Ensure sorting result.
        job.setMapOutputKeyClass(RankKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job, new Path(output));
        TextOutputFormat.setOutputPath(job, new Path(rankResult));
        //Write the time result to time result path.
        endTime = startTime - System.currentTimeMillis();
        time = time + "Total time is: " + endTime;
        fs.delete(new Path(timeResult), true);
        FSDataOutputStream outStream = fs.create(new Path(timeResult));
        outStream.write(time.getBytes());
        outStream.close();

        status = job.waitForCompletion(true);
        System.exit(status? 0 : 1);
    }
}







