import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.net.URI;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

public class WordConfCount{
	
	public static class TokenizerMapper
		extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private boolean caseSensitive;
		private Set<String> patternsToSkip = new HashSet<String>();
		private Configuration conf;
		private BufferedReader fis;

		//Places necessary control information into variables
		public void setup(Context context) throws IOException, InterruptedException {
			conf = context.getConfiguration();
			caseSensitive = conf.getBoolean("wordcount.case.sensitive", true);
			if(conf.getBoolean("wordcount.skip.patterns", false))
			{
				URI[] patternURIs = Job.getInstance(conf).getCacheFiles();
				for(URI patternURI : patternURIs)
				{
					Path patternPath = new Path(patternURI.getPath());
					String patternFileName = patternPath.getName().toString();
					parseSkipFile(patternFileName);
				}
			}
		}

		//Reads -skip file and adds patterns that are skipped
		public void parseSkipFile(String fileName){
			try {
				fis = new BufferedReader(new FileReader(fileName));
				String pattern = null;
				while((pattern = fis.readLine()) != null){
					patternsToSkip.add(pattern);
				}
			} catch (IOException ioe) {
				System.err.println("Error parsing skipfile"
					+ StringUtils.stringifyException(ioe));
			}
		}

		//first map iteration
		//Counts number of lines a given word or pair of words occurs on
		public void map(Object key, Text value, Context context
			       ) throws IOException, InterruptedException {
			ArrayList<String> A = new ArrayList<String>();
			String line = (caseSensitive) ?
				value.toString() : value.toString().toLowerCase();
			for(String pattern : patternsToSkip){
				line = line.replaceAll(pattern, " ");
			}
			StringTokenizer itr = new StringTokenizer(line);
			while(itr.hasMoreTokens())
			{
				String temp = itr.nextToken();
				if(A.contains(temp) == false){
					A.add(temp);
				}
			}
			Collections.sort(A);
			for(int i = 0; i < A.size(); i++)
			{
				String str1 = A.get(i);
				word.set(str1);
				context.write(word, one);
				for(int j = i+1; j < A.size(); j++)
				{
					String str2 = A.get(j);
					String submit = str1 + ":" + str2;
					word.set(submit);
					context.write(word, one);
				}
			}
		}
	}

	public static class ConfMap
		extends Mapper<Object, Text, Text, Text>{
		
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text word2 = new Text();
		
		//second map iteration
		//Gathers information from 1st stage reducer, compiles in a Text
		//and writes to context
		public void map(Object key, Text value, Context context
			       ) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			String keyString;

			String item = itr.nextToken();
			int num = Integer.parseInt(itr.nextToken());
			
			//determines key
			if(item.contains(":"))
			{
				keyString = item.substring(0, item.lastIndexOf(':'));
			}
			else
			{
				keyString = item;
			}
			word.set(keyString);
			
			String temp = item + ";" + num;
			word2.set(temp);
			context.write(word, word2);

			//flips a:b -> b:a to note both directions of confidence
			if(item.contains(":")){
				temp = item.substring(item.lastIndexOf(':')+1) + ":" + keyString + ";" + num;
				word2.set(temp);
				temp = item.substring(item.lastIndexOf(":")+1);
				word.set(temp);
				context.write(word, word2);
			}
		}
	}
	
	public static class ConfReducer
		extends Reducer<Text, Text, Text, DoubleWritable>{
		
		//key is "word1"
		//value is "word1:word2;numword1word2"
		//or "word1;numword1"
		
		//output     "word1:word2"      DoubleWritable confidence

		private DoubleWritable result = new DoubleWritable();
		private Text word = new Text();
		
		//second stage reducer
		//pulls number of key and number of pair to calculate confidence
		//and writes calculation to context
		public void reduce(Text key, Iterable<Text> values, Context context
				) throws IOException, InterruptedException {
			double keySum = 0;
			double comboSum = 0;
			String temp = "";

			//use arraylist to allow for multiple iterations
			ArrayList<String> valueList = new ArrayList<String>();
			for(Text val : values){
				valueList.add(val.toString());
			}

			//Measure number of key
			for(String val : valueList){
				if(!val.contains(":")){
					keySum = Double.parseDouble(val.substring(val.lastIndexOf(';')+1));
				}
			}

			//Measure number of all other pairs and calculate confidence
			for(String val : valueList){
				if(val.contains(":")){
					String pair = val.substring(0, val.lastIndexOf(';'));
					String pairNum = val.substring(val.lastIndexOf(';')+1);
					comboSum = Integer.parseInt(pairNum);

					word.set(pair);
					double conf = comboSum/keySum;
					result.set(conf);
					context.write(word, result);
				}
			}
		}
			
	}
	
	//First stage reducer
	//sums up number of each word and each pair within the document
	public static class IntSumReducer
		extends Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
		String[] remainingArgs = optionParser.getRemainingArgs();
		if((remainingArgs.length != 3) && (remainingArgs.length != 5)) {
			System.err.println("Usage: wordcount <in> <out> <out2> [-skip skipPatternFile]");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordConfCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		List<String> otherArgs = new ArrayList<String>();
		for(int i = 0; i < remainingArgs.length; i++){
			if("-skip".equals(remainingArgs[i])){
				job.addCacheFile(new Path(remainingArgs[++i]).toUri());
				job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
				otherArgs.add(remainingArgs[i]);
			}
		}
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.waitForCompletion(true); //first MapReduce job finishes here

		Configuration confTwo = new Configuration();
 		Job job2 = Job.getInstance(confTwo, "Conf step two");
 		job2.setJarByClass(WordConfCount.class);
 		job2.setMapperClass(ConfMap.class);
 		job2.setReducerClass(ConfReducer.class);
 		job2.setOutputKeyClass(Text.class);
 		job2.setOutputValueClass(Text.class);
 		FileInputFormat.addInputPath(job2, new Path(args[2]));
 		FileOutputFormat.setOutputPath(job2, new Path(args[3]));
		
		job2.waitForCompletion(true); //second MapReduce job finishes 

		System.exit(job2.waitForCompletion(true) ? 0:1); //exit
	}
}
