import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.*;

/*
 * Main class of the TFICF MapReduce implementation.
 * Author: Tyler Stocksdale
 * Date:   10/18/2017
 */
public class TFICF {

    public static void main(String[] args) throws Exception {
        // Check for correct usage
        if (args.length != 2) {
            System.err.println("Usage: TFICF <input corpus0 dir> <input corpus1 dir>");
            System.exit(1);
        }
		
		// return value of run func
		int ret = 0;
		
		// Create configuration
		Configuration conf0 = new Configuration();
		Configuration conf1 = new Configuration();
		
		// Input and output paths for each job
		System.out.println(args[0]);
		System.out.println(args[1]);

		Path inputPath0 = new Path(args[0]);
		Path inputPath1 = new Path(args[1]);

		/* First map reduce is run on the first input and if it succeeds then it is 
		run on the second input */
        try{
			System.out.println("Going to start first");
            ret = run(conf0, inputPath0, 0);
			System.out.println("Returned from first");
			System.out.println(ret);
        }catch(Exception e){
            e.printStackTrace();
        }
        if(ret == 0){
			System.out.println("Starting second");
        	try{
            	run(conf1, inputPath1, 1);
        	}catch(Exception e){
            	e.printStackTrace();
        	}        	
        }
     
     	System.exit(ret);
    }
		
	public static int run(Configuration conf, Path path, int index) throws Exception{
		// Input and output paths for each job

		Path wcInputPath = path;
		Path wcOutputPath = new Path("output" +index + "/WordCount");
		Path dsInputPath = wcOutputPath;
		Path dsOutputPath = new Path("output" + index + "/DocSize");
		Path tficfInputPath = dsOutputPath;
		Path tficfOutputPath = new Path("output" + index + "/TFICF");
		
		// Get/set the number of documents (to be used in the TFICF MapReduce job)
        FileSystem fs = path.getFileSystem(conf);
        FileStatus[] stat = fs.listStatus(path);
		String numDocs = String.valueOf(stat.length);
		conf.set("numDocs", numDocs);
		
		// Delete output paths if they exist
		FileSystem hdfs = FileSystem.get(conf);
		if (hdfs.exists(wcOutputPath))
			hdfs.delete(wcOutputPath, true);
		if (hdfs.exists(dsOutputPath))
			hdfs.delete(dsOutputPath, true);
		if (hdfs.exists(tficfOutputPath))
			hdfs.delete(tficfOutputPath, true);
		
		// Create and execute Word Count job
		
			/************ YOUR CODE HERE ************/
			Job wcMapperJob = Job.getInstance(conf, "wcMapperJob");
			wcMapperJob.setJarByClass(TFICF.class);
			wcMapperJob.setMapperClass(WCMapper.class);
			wcMapperJob.setCombinerClass(WCReducer.class); // Needed? What is this?
			wcMapperJob.setReducerClass(WCReducer.class);
			wcMapperJob.setOutputKeyClass(Text.class);
			wcMapperJob.setOutputValueClass(IntWritable.class);
			FileInputFormat.addInputPath(wcMapperJob, wcInputPath);
			FileOutputFormat.setOutputPath(wcMapperJob, wcOutputPath);
			wcMapperJob.waitForCompletion(true);

			// System.exit(wcMapperJob.waitForCompletion(true) ? 0 : 1);
		// Create and execute Document Size job
		
			Job dsMapperJob = Job.getInstance(conf, "dsMapperJob");
			dsMapperJob.setJarByClass(TFICF.class);
			dsMapperJob.setMapperClass(DSMapper.class);
			dsMapperJob.setReducerClass(DSReducer.class);
			dsMapperJob.setOutputKeyClass(Text.class);
			dsMapperJob.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(dsMapperJob, dsInputPath);
			FileOutputFormat.setOutputPath(dsMapperJob, dsOutputPath);
			// dsMapperJob.waitForCompletion(true);

			/************ YOUR CODE HERE ************/
		
		//Create and execute TFICF job
		
			/************ YOUR CODE HERE ************/

		//Return final job code , e.g. retrun tficfJob.waitForCompletion(true) ? 0 : 1
			/************ YOUR CODE HERE ************/
		return(dsMapperJob.waitForCompletion(true) ? 0 : 1);
    }
	
	/*
	 * Creates a (key,value) pair for every word in the document 
	 *
	 * Input:  ( byte offset , contents of one line )
	 * Output: ( (word@document) , 1 )
	 *
	 * word = an individual word in the document
	 * document = the filename of the document
	 */
	public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
		// public static final Log log = LogFactory.getLog(WCMapper.class);
		/************ YOUR CODE HERE ************/
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {

		// System.out.println(value.toString());

		String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

		String line = value.toString().replaceAll("([^\\P{P}\\-\\[\\]\\xBF\\/]|\\=)+", "").toLowerCase();
		// line = line.toString().replaceAll("^[0-9]+$", "");
		// System.out.println(line);
		String wordSetString;
		StringTokenizer itr = new StringTokenizer(line);
		String token;
		boolean onlyNumberCheck;
		boolean startsWithLetterCheck;
		boolean bracketWithOnlyLetterCheck;

		while (itr.hasMoreTokens()) {
			token = itr.nextToken();

			//Checks
			// onlyNumberCheck = token.matches("[0-9]+");
			// if(onlyNumberCheck){
			// 	continue;
			// }
			// System.out.println(token);
			// if(token.matches("^.*=.*$")){
			// 	token = token.replaceAll("^.*(=).*", "");
			// 	System.out.println(token);
			// }
			startsWithLetterCheck = token.matches("^[a-z]+.*$");
			if(!startsWithLetterCheck){
				continue;
			}

			// bracketWithOnlyLetterCheck = token.matches();
			// if(bracketWithOnlyLetterCheck){
			// 	continue;
			// }
			//
			wordSetString = String.format("%s@%s", token, fileName);
			word.set(wordSetString);
			/* public void write(KEYOUT key, VALUEOUT value) */
			// System.out.println(wordSetString);
			context.write(word, one);
		}

			// log.info("WhiteChocolateMocha");
		}
		
    }

    /*
	 * For each identical key (word@document), reduces the values (1) into a sum (wordCount)
	 *
	 * Input:  ( (word@document) , 1 )
	 * Output: ( (word@document) , wordCount )
	 *
	 * wordCount = number of times word appears in document
	 */
	public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
    }
	
	/*
	 * Rearranges the (key,value) pairs to have only the document as the key
	 *
	 * Input:  ( (word@document) , wordCount )
	 * Output: ( document , (word=wordCount) )
	 */
	public static class DSMapper extends Mapper<Object, Text, Text, Text> {
		
		/************ YOUR CODE HERE ************/
		private final static IntWritable one = new IntWritable(1);

		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
			
			String word;
			String document;
			String count;

			Text wordKey = new Text();
			Text wordValue = new Text();

			String line = value.toString();
			String[] splitLine = line.split("\\s+");
			// splitLine[0] is word@document | splitLine[1] is count;
	
			String[] wordDocument = splitLine[0].split("\\@");
			// //wordDocument[0] is the word | wordDocument[1] is the document

			String outputString;
			
			outputString = String.format("%s=%s", wordDocument[0], splitLine[1]);


			wordKey.set(wordDocument[1]);
			wordValue.set(outputString);
			// wordKey.set(wordDocument[1]);
			// wordValue.set(outputString);
			System.out.println("Done with DSMapper");
			context.write(wordKey, wordValue);
		}
    }

    /*
	 * For each identical key (document), reduces the values (word=wordCount) into a sum (docSize) 
	 *
	 * Input:  ( document , (word=wordCount) )
	 * Output: ( (word@document) , (wordCount/docSize) )
	 *
	 * docSize = total number of words in the document
	 */
	public static class DSReducer extends Reducer<Text, Text, Text, Text> {
		
		/************ YOUR CODE HERE ************/
		public void reduce(Text document, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			/* We get document = documentName and values = list of word=wordCount values in that file */
			int docSize = 0;
			String outputKey;
			String outputValue;

			System.out.println("Starting with DSReducer");
			System.out.println(document);

			List<String> backup = new ArrayList<String>();

			Iterator<Text> iter = values.iterator();
			System.out.println("InputValues:");
			for(Text v : values){
				System.out.println(v.toString());
				backup.add(v.toString());
				docSize += Integer.parseInt(v.toString().split("=")[1]);
			}

			System.out.println(docSize);

			System.out.println("Klose");
			for(String val : backup){
				System.out.println("Mango");
				String[] splitValues = val.split("=");

				outputKey = String.format("%s@%s", splitValues[0], document.toString());
				outputValue = String.format("%s/%s", splitValues[1], String.valueOf(docSize));
				System.out.println(outputKey);
				System.out.println(outputValue);

				Text wordKey = new Text();
				Text wordValue = new Text();

				wordKey.set(outputKey);
				wordValue.set(outputValue);

				context.write(wordKey, wordValue);
			}

		}
		
    }
	
	/*
	 * Rearranges the (key,value) pairs to have only the word as the key
	 * 
	 * Input:  ( (word@document) , (wordCount/docSize) )
	 * Output: ( word , (document=wordCount/docSize) )
	 */
	public static class TFICFMapper extends Mapper<Object, Text, Text, Text> {

		/************ YOUR CODE HERE ************/
		
    }

    /*
	 * For each identical key (word), reduces the values (document=wordCount/docSize) into a 
	 * the final TFICF value (TFICF). Along the way, calculates the total number of documents and 
	 * the number of documents that contain the word.
	 * 
	 * Input:  ( word , (document=wordCount/docSize) )
	 * Output: ( (document@word) , TFICF )
	 *
	 * numDocs = total number of documents
	 * numDocsWithWord = number of documents containing word
	 * TFICF = ln(wordCount/docSize + 1) * ln(numDocs/numDocsWithWord +1)
	 *
	 * Note: The output (key,value) pairs are sorted using TreeMap ONLY for grading purposes. For
	 *       extremely large datasets, having a for loop iterate through all the (key,value) pairs 
	 *       is highly inefficient!
	 */
	public static class TFICFReducer extends Reducer<Text, Text, Text, Text> {
		
		private static int numDocs;
		private Map<Text, Text> tficfMap = new HashMap<>();
		
		// gets the numDocs value and stores it
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			numDocs = Integer.parseInt(conf.get("numDocs"));
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			/************ YOUR CODE HERE ************/
	 
			//Put the output (key,value) pair into the tficfMap instead of doing a context.write
			// tficfMap.put(/*document@word*/, /*TFICF*/);
		}
		
		// sorts the output (key,value) pairs that are contained in the tficfMap
		protected void cleanup(Context context) throws IOException, InterruptedException {
            Map<Text, Text> sortedMap = new TreeMap<Text, Text>(tficfMap);
			for (Text key : sortedMap.keySet()) {
                context.write(key, sortedMap.get(key));
            }
        }
		
    }
}
