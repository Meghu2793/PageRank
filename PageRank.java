/* Name	: Megha Krishnamurthy
 * Student ID	: 800974844
 */
package org.myorg;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class PageRank extends Configured implements Tool {

 public static void main(String[] args) throws Exception {
  int res = ToolRunner.run(new PageRank(), args);
  System.exit(res);
 }
 public int run(String[] args) throws Exception {

//  Job1 does the linecount  
  Job job = Job.getInstance(getConf(), "countLines");
  job.setJarByClass(this.getClass());

  FileInputFormat.addInputPaths(job, args[0]);
  FileOutputFormat.setOutputPath(job, new Path(args[1] + "job1"));

  job.setMapperClass(countLinesmap.class); //Setting the mapper class 
  job.setReducerClass(countLinesreduce.class); //Setting the reducer class

  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(IntWritable.class);

  int j1status = job.waitForCompletion(true) ? 0 : 1; // check if the job ran properly or not
  if (j1status == 0) {
   int num_nodes = 1;
   int j3status = 1;


   Configuration conf2 = new Configuration();
   FileSystem fs2 = FileSystem.get(conf2);
   Path p = new Path(args[1] + "job1", "part-r-00000");
   BufferedReader bf = new BufferedReader(new InputStreamReader(fs2.open(p)));

   String line;
   while ((line = bf.readLine()) != null) {
    if (line.trim().length() > 0) {
     System.out.println(line);

     String[] parts = line.split("\\s+");
     num_nodes = Integer.parseInt(parts[1]);
    }
   }
   bf.close();


    //Job2 calculates the link graph
   Job job2 = Job.getInstance(getConf(), "linkgraph");
   job2.setJarByClass(this.getClass());
   Path job2path = new Path(args[1] + "job2" + "0");
   FileInputFormat.addInputPaths(job2, args[0]);
   FileOutputFormat.setOutputPath(job2, job2path);
   job2.getConfiguration().setInt("num_nodes1", num_nodes);

   job2.setMapperClass(linkgraphmap.class); //Setting the mapper class 
   job2.setReducerClass(linkgraphreduce.class); //Setting the reducer class

   job2.setOutputKeyClass(Text.class);
   job2.setOutputValueClass(Text.class);

   int j2status = job2.waitForCompletion(true) ? 0 : 1; // check if the job ran properly or not

   if (j2status == 0) {



    int i = 0;
    int loop = 0;

    while (loop == 0) {
     i++;
     //Job3 job for pagernk
     Job job3 = Job.getInstance(getConf(), "pagerank");
     job3.setJarByClass(this.getClass());

     Path job2apath = new Path(args[1] + "job2" + (i - 1));
     Path job3path = new Path(args[1] + "job2" + i);

     FileInputFormat.addInputPath(job3, job2apath);
     FileOutputFormat.setOutputPath(job3, job3path);

     job3.setMapperClass(pagerankMap.class); //Setting the mapper class 
     job3.setReducerClass(pagerankReduce.class); //Setting the reducer class

     job3.setOutputKeyClass(Text.class);
     job3.setOutputValueClass(Text.class);


     j3status = job3.waitForCompletion(true) ? 0 : 1; // check if the job ran properly or not
     if (i > 1) {
      loop = (int) job3.getCounters().findCounter("count", "count").getValue();

     }

     if (fs2.exists(new Path(args[1] + "job2" + (i - 1)))) {
      fs2.delete(new Path(args[1] + "job2" + (i - 1)), true);
     }

    }

    //Job4 sorts according to pagerank
    if (j3status == 0) {
     Job job4 = Job.getInstance(getConf(), "sorting");
     job4.setJarByClass(this.getClass());

     String job4path = args[1] + "job2" + i;
     Path job4apath = new Path(args[1]);


     FileInputFormat.addInputPaths(job4, job4path);
     FileOutputFormat.setOutputPath(job4, job4apath);
     job4.setNumReduceTasks(1);
     job4.setInputFormatClass(KeyValueTextInputFormat.class);
     job4.setOutputFormatClass(TextOutputFormat.class);

     job4.setMapperClass(sortingmap.class); //Setting the mapper class 
     job4.setReducerClass(sortingreduce.class); //Setting the reducer class

     job4.setSortComparatorClass(cmp.class);

     job4.setMapOutputKeyClass(DoubleWritable.class);
     job4.setMapOutputValueClass(Text.class);

     job4.setOutputKeyClass(Text.class);
     job4.setOutputValueClass(DoubleWritable.class);


     int j4status = job4.waitForCompletion(true) ? 0 : 1; // check if the job ran properly or not

     if (fs2.exists(new Path(args[1] + "job2" + (i)))) {
      fs2.delete(new Path(args[1] + "job2" + (i)), true);
     }
     if (fs2.exists(new Path(args[1] + "job1"))) {
      fs2.delete(new Path(args[1] + "job1"), true);
     }
    }
   }
  }
  return 0;
 }


// This is a map function for counting the number of files/lines in the document
 public static class countLinesmap extends Mapper < LongWritable, Text, Text, IntWritable > {
  private final static IntWritable one = new IntWritable(1);
  public void map(LongWritable offset, Text lineText, Context context)
  throws IOException,
  InterruptedException {
   String line = lineText.toString();
   if (line.trim().length() > 0) {
    context.write(new Text("countLines"), one);
   }
  }
 }

//this is a Reduce function for counting the number of files/lines in the document
 public static class countLinesreduce extends Reducer < Text, IntWritable, Text, IntWritable > {
  @Override
  public void reduce(Text line, Iterable < IntWritable > values, Context context)
  throws IOException,
  InterruptedException {
   int sum = 0;
   for (IntWritable value: values) {
    sum = sum + value.get();
   }
   context.write(line, new IntWritable(sum));
  }
 }

//This is a mapper to create the link graph given the input file
 public static class linkgraphmap extends Mapper < LongWritable, Text, Text, Text > {
  final Pattern title = Pattern.compile("<title>(.+?)</title>");
  final Pattern linkPattern = Pattern.compile("\\[\\[.*?]\\]");
  public void map(LongWritable offset, Text lineText, Context context)
  throws IOException,
  InterruptedException {
   String line = lineText.toString();
   if (line.trim().length() > 0) {
    Matcher regex = title.matcher(line);
    regex.find();
    String key = regex.group(0);
    if (key != null && !key.isEmpty()) {
     Matcher m = linkPattern.matcher(line);

     while (m.find()) {
      String url = m.group().replace("[[", "").replace("]]", "");
      if (!url.isEmpty()) {
       context.write(new Text(key), new Text(url));

      }
     }

    }
   }
  }

 }


//This is a reducer to create the link graph given the input file
 public static class linkgraphreduce extends Reducer < Text, Text, Text, Text > {
  @Override
  public void reduce(Text line, Iterable < Text > values, Context context)
  throws IOException,
  InterruptedException {
   int rank = context.getConfiguration().getInt("num_nodes1", 1);
   double initialpagerank = 1 / (double) rank;
   String OV = initialpagerank + "</psp>";

   for (Text value: values) {
    OV = OV + value.toString() + "</usp>";
   }

   context.write(line, new Text("<text>" + OV + "</text>"));
  }
 }


//This mapper helps in calculating the pagerank
 public static class pagerankMap extends Mapper < LongWritable, Text, Text, Text > {

  public void map(LongWritable offset, Text lineText, Context context)
  throws IOException,
  InterruptedException {
   String line = lineText.toString(); //convert to string
   String urllist = StringUtils.substringBetween(line, "<text>", "</text>");
   String url = StringUtils.substringBetween(line, "<title>", "</title>");
   String[] firstpart = urllist.split("</psp>");
   double initialpagerank = Double.parseDouble(firstpart[0]);
   if (firstpart.length > 1) {
    String lnk = firstpart[1];
    if (!lnk.isEmpty()) {
     String[] out_links = lnk.split("</usp>");
     double newpagerank = initialpagerank / (double) out_links.length;
     for (String U: out_links) {
      context.write(new Text(U), new Text(newpagerank + ""));

     }

     context.write(new Text(url), new Text(initialpagerank + "</psp>" + lnk));

    }
   }

  }
 }


//This reducer calculates the pagerank
 public static class pagerankReduce extends Reducer < Text, Text, Text, Text > {

  private static double DAMPING_FACTOR = 0.85;
  @Override
  protected void setup(Reducer < Text, Text, Text, Text > .Context context)
  throws IOException,
  InterruptedException {

   super.setup(context);
   context.getCounter("count", "count").setValue(1);
  }
  @Override
  public void reduce(Text UNode, Iterable < Text > values, Context context)
  throws IOException,
  InterruptedException {

   double sumpagerank = 0.0;
   double oldpagerank = 0.0;
   String val = "";
   String prv = null;
   for (Text value: values) {
    String nv = value.toString();
    if (nv.contains("</usp>")) {
     String[] ppart = nv.split("</psp>");
     if (ppart.length > 1) {
      oldpagerank = Double.parseDouble(ppart[0]);
      prv = ppart[1];
     }
    } else {
     sumpagerank = sumpagerank + Double.parseDouble(nv);
    }

   }

   if (prv != null) {
    double newpagerank = (1.0 - DAMPING_FACTOR) + (DAMPING_FACTOR * sumpagerank);
    String prval = "<text>" + newpagerank + "</psp>" + prv + "</text>";

    if ((Math.abs(oldpagerank - newpagerank)) > 0.0001) {
     context.getCounter("count", "count").setValue(0);

    }

    String output = UNode.toString();

    context.write(new Text("<title>" + output + "</title>"), new Text(prval));
   }
  }

 }



//Using the comparator for sorting
 public static class cmp extends WritableComparator {

  protected cmp() {
   super(DoubleWritable.class, true);
  }

  public int compare(@SuppressWarnings("rawtypes") WritableComparable key1, @SuppressWarnings("rawtypes") WritableComparable key2) {

   DoubleWritable x = (DoubleWritable) key1;
   DoubleWritable y = (DoubleWritable) key2;

   int comp = x.compareTo(y);

   return -1 * comp;
  }

 }


 //Mapper for sorting. using exchange method for sorting
 public static class sortingmap extends Mapper < Text, Text, DoubleWritable, Text > {

  public void map(Text key, Text lineText, Context context)
  throws IOException,
  InterruptedException {

   String ks = key.toString();
   String line = lineText.toString();

   String title = StringUtils.substringBetween(ks, "<title>", "</title>");
   String urls = StringUtils.substringBetween(line, "<text>", "</text>");

   String[] line_parts = urls.split("</psp>");
   double page_rank = 0.0;
   if (line_parts.length > 0) {

    page_rank = Double.parseDouble(line_parts[0]);
   }

   if (line_parts != null && title != null) {

    context.write(new DoubleWritable(page_rank), new Text(title));
   }
  }
 }


 //This is a reducer for sorting
 public static class sortingreduce extends Reducer < DoubleWritable, Text, Text, DoubleWritable > {
  @Override
  public void reduce(DoubleWritable rank, Iterable < Text > nodes, Context context)
  throws IOException,
  InterruptedException {
    
    for (Text node: nodes) {
    context.write(node, rank);
   }
  }
 }
}
