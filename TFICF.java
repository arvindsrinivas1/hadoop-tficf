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

		Path inputPath0 = new Path(args[0]);
		Path inputPath1 = new Path(args[1]);

		/* First map reduce is run on the first input and if it succeeds then it is 
		run on the second input */
        try{
            ret = run(conf0, inputPath0, 0);
        }catch(Exception e){
            e.printStackTrace();
        }
        if(ret == 0){
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
			Job wcMapperJob = Job.getInstance(conf, "wcMapperJob");
			wcMapperJob.setJarByClass(TFICF.class);
			wcMapperJob.setMapperClass(WCMapper.class);
			wcMapperJob.setCombinerClass(WCReducer.class); //To DO:  Needed? What is this?
			wcMapperJob.setReducerClass(WCReducer.class);
			wcMapperJob.setOutputKeyClass(Text.class);
			wcMapperJob.setOutputValueClass(IntWritable.class);

			/* Info: FileInputFormat class has the computeInputSplit function that decides the 
			size of each input split. Each inputSplit is worked on by a mapper task */
			FileInputFormat.addInputPath(wcMapperJob, wcInputPath);
			FileOutputFormat.setOutputPath(wcMapperJob, wcOutputPath);

			wcMapperJob.waitForCompletion(true);

		// Create and execute Document Size job
			Job dsMapperJob = Job.getInstance(conf, "dsMapperJob");
			dsMapperJob.setJarByClass(TFICF.class);
			dsMapperJob.setMapperClass(DSMapper.class);
			dsMapperJob.setReducerClass(DSReducer.class);
			dsMapperJob.setOutputKeyClass(Text.class);
			dsMapperJob.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(dsMapperJob, dsInputPath);
			FileOutputFormat.setOutputPath(dsMapperJob, dsOutputPath);
			dsMapperJob.waitForCompletion(true);

		// 	/************ YOUR CODE HERE ************/
		
		// //Create and execute TFICF job
			Job tficfMapperJob = Job.getInstance(conf, "tficfMapperJob");
			tficfMapperJob.setJarByClass(TFICF.class);
			tficfMapperJob.setMapperClass(TFICFMapper.class);
			tficfMapperJob.setReducerClass(TFICFReducer.class);
			tficfMapperJob.setOutputKeyClass(Text.class);
			tficfMapperJob.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(tficfMapperJob, tficfInputPath);
			FileOutputFormat.setOutputPath(tficfMapperJob, tficfOutputPath);
			return(tficfMapperJob.waitForCompletion(true) ? 0 : 1);
			/************ YOUR CODE HERE ************/

		//Return final job code , e.g. retrun tficfJob.waitForCompletion(true) ? 0 : 1
			/************ YOUR CODE HERE ************/
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

	/* Notes about mappers:
		Input files are split into logical units of inputSplits that have some k-v pairs(records in each).
		Each inputSplit is picked up and worked by a MapperTask
		InputSplit Class stores information about the location of the inputSplit chunks. This helps in spawning
		the mapper tasks close to the location of data (DataLocality)
	*/


	/* Each inputSplit will have some lines(records) in it. Here, it looks like each record is handled by a different mapper task.
	Key Value check print function gives:
		WCMAPPER
		10 ---->(Key: Offset of that line in that file
		They come from everywhere (The line)

		Output is ( (word@document) , 1 ). So aggregation will be done based on word@document).
		TIP to self : So, think about what your mapper k-v output should be by thinking about on what value we want 
		aggreagation to be done

		Try to think about what kind of data manipulation/processing my mapper should do so that it can produce
		an intermediate k-v pair on which the reducer can work to produce an aggregated result
	*/
	public static class WCMapper extends Mapper<Object, Text, Text, IntWritable> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


			/* Check what Key and value values are 
			System.out.println("WCMAPPER");
			System.out.println(key);
			System.out.println(value);
			*/

			/* Source: https://stackoverflow.com/questions/19012482/how-to-get-the-input-file-name-in-the-mapper-in-a-hadoop-program */
			String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();


			/* Replace all:
			^ -> not
			\P -> not a punctuation
			-, [,], xBF, = -> characters that we want to retain
			| = -> = is not considered in \p so we have to give that seperately
			Hence, replace all not(not a punctuation, -, [,],xBF, =) => Replace all (punctuation, not -, not [ ......)
			That is, all punctuations except [,], - , xBF, = are removed

			*/

			// String line = value.toString().replaceAll("^\\[.*\\]$", "").toLowerCase();
			// line = line.replaceAll("\\s+\\[.*\\]", "").toLowerCase();

			/* Note 1: We cant split(tokenize) it first and then remove punctuations because we would lose 
			cases like [chorus repeat x2] */
			//String line = value.toString().replaceAll("([^\\P{P}\\-\\[\\]\\xBF]|\\=)+", "").toLowerCase();
			StringTokenizer itr = new StringTokenizer(value.toString(), " ");
			/* replace [] tha are preceeded by a  space but don't delete it if it is preceede dby another character */
			// line = line.replaceAll("^\\[.*\\]$", "");
			// line = line.replaceAll("\\s+\\[.*\\]", "");


			// line = line.toString().replaceAll("^[0-9]+$", "");
			String wordSetString;
			// StringTokenizer itr = new StringTokenizer(line);
			String token;
			boolean onlyNumberCheck;
			boolean startsWithLetterCheck;
			boolean bracketWithOnlyLetterCheck;

			Text word = new Text();
			IntWritable wordValue = new IntWritable();
			wordValue.set(1);

			while (itr.hasMoreTokens()) {
				token = itr.nextToken();
				System.out.println("Before:");
				System.out.println(token);
				token = token.replaceAll("([^\\P{P}\\-\\[\\]\\xBF\\/]|\\=)+", "").toLowerCase();
				System.out.println("After:");
				System.out.println(token);
				//Checks
				// onlyNumberCheck = token.matches("[0-9]+");
				// if(onlyNumberCheck){
				// 	continue;
				// }
				// if(token.matches("^.*=.*$")){
				// 	token = token.replaceAll("^.*(=).*", "");
				// }
				startsWithLetterCheck = token.matches("^[a-z]+.*$");
				if(!startsWithLetterCheck){
					// System.out.println(token);
					continue;
				}

				// line = line.replaceAll("^\\[.*\\]$", "");
				// line = line.replaceAll("\\s+\\[.*\\]", "");
				// token = token.replaceAll("[", "");

				// At this stage we have cleared words that start with [ and with ].
				// But we might have words that have [] inside them ex: he[ll]o.
				// For these words we just have to remove the [ and ]
				token = token.replaceAll("[\\[\\]]", "");

				// bracketWithOnlyLetterCheck = token.matches();
				// if(bracketWithOnlyLetterCheck){
				// 	continue;
				// }
				//
				wordSetString = String.format("%s@%s", token, fileName);
				word.set(wordSetString);
				/* public void write(KEYOUT key, VALUEOUT value) */
				context.write(word, wordValue);
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


	 /* This aggregates the input on the basis of it's key, which is the word@document in this case */
	 /* Each agregrated chunk will be processed by a reducer task */
	 /* WCREDUCER
	 	a@bruce-springsteen.txt ----> (Key)
		(Number of 1s) ->(values)
	*/
	public static class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			
			/*  Iterable<IntWritable> values will have 1s for each count of that word in that document,
			that is, we group based on word@document as the key. Sum it up to get the total count of that 
			word in the document. Each reducer task does this sum aggregration on count  */
			for (IntWritable val : values) {
				// System.out.println(val);
				sum += val.get();
			}

			IntWritable outputValue = new IntWritable();
			outputValue.set(sum);

			context.write(key, outputValue);
		}
    }
	
	/*
	 * Rearranges the (key,value) pairs to have only the document as the key
	 *
	 * Input:  ( (word@document) , wordCount )
	 * Output: ( document , (word=wordCount) )
	 */

	 /* Key-> OFfset of the line
	 Value -> line 
	 */
	public static class DSMapper extends Mapper<Object, Text, Text, Text> {
		
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
	
			String[] wordDocument = splitLine[0].split("@");
			// //wordDocument[0] is the word | wordDocument[1] is the document

			String outputString;
			
			outputString = String.format("%s=%s", wordDocument[0], splitLine[1]);


			wordKey.set(wordDocument[1]);
			wordValue.set(outputString);

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
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			/* We get document = documentName and values = list of word=wordCount values in that file */
			int docSize = 0;
			String outputKey;
			String outputValue;

			/* The Iterable object is only iterable once. Hence, since we are using it to count the docSize
			we have to save the values in another datastructure so that we can use it another time.*/
			List<String> backup = new ArrayList<String>();

			Iterator<Text> iter = values.iterator();
			// NO idea why this doesnt work. Check later. Iterable is weird.
			// while(iter.hasNext()){
			// 	backup.add(iter.next().toString());
			// 	docSize += Integer.parseInt(iter.next().toString().split("=")[1]);
			// }
			for(Text v : values){
				System.out.println(v);
				backup.add(v.toString());
				docSize += Integer.parseInt(v.toString().split("=")[1]);
			}

			for(String val : backup){
				String[] splitValues = val.split("=");

				outputKey = String.format("%s@%s", splitValues[0], key.toString());
				outputValue = String.format("%s/%s", splitValues[1], String.valueOf(docSize));

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
		public void map(Object key, Text value, Context context
		) throws IOException, InterruptedException {
	
			/* Sxystem.out.println(value.toString()); filled@al-green.txt	2/16597 */
			String line = value.toString();
			String[] splitLine = line.split("\\s+");
			/* splitLine[0] = word@document | splitLine[1] = wc/docSize */

			String[] w = splitLine[0].split("@");
			/*w[0] = word | w[1] = document */
			String[] d = splitLine[1].split("/");
			/* d[0] = wc | d[1] = docSize */

			String outputString = String.format("%s=%s/%s", w[1], d[0], d[1]);

			Text outputKey = new Text();
			Text outputValue = new Text();

			outputKey.set(w[0]);
			outputValue.set(outputString);

			context.write(outputKey, outputValue);
		}
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


	// wij =log(1+fij)×log(N +1/nj + 1)
	
	/* wij -> Weight of term j in document i
	   N -> Number of documents in the corpus 
	   j -> Each term j
	   nj -> number of documents in the corpus the term j occured once or more
	   fij is the number of occurrences of term j in document i (TF)
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
			// int wordCount = Integer.parseInt();

			/* The shuffler(or something else in hadoop) somehow magically groups based on key and that is given to 
			each of the reducer workers. Input for each reduce() calls is something like:
			Key: a
			Values: janisjoplin.txt=329/21502
					bruce-springsteen.txt=543/21436
					al-green.txt=209/16597 */
			int number_of_documents_with_word = 0;
			List<String> backup = new ArrayList<String>();

			for(Text val : values){
				backup.add(val.toString());
				++number_of_documents_with_word; //nj
			}

			double wij;
			double tf;
			double icf;

			int wcCalc;
			int dsCalc;


			for(String val2 : backup){
				/* We have to calculate fij which is the frequence of the word in that document.
				So for each value of val2(ie each word/document pair), we have to calculate wij and
				put it in the hashmap */

				String[] document_frequence_split = val2.split("=");
				/* document_f_split[0] -> document name
				   document_f_split[1] -> wordCount/docSize
				*/
				String outputString;
				outputString = String.format("%s@%s", document_frequence_split[0], key.toString());

				String[] wordCountBydocSize = document_frequence_split[1].split("/");
				/* wordCountBydocSize[0] -> wordCount (in that document)
				   wordCountBydocSize[1] -> docSize
				*/
				wcCalc = Integer.parseInt(wordCountBydocSize[0]);
				dsCalc = Integer.parseInt(wordCountBydocSize[1]);

				tf = Math.log(1 + ((double)wcCalc/dsCalc));
				icf = Math.log((double)(numDocs + 1)/(number_of_documents_with_word + 1));
				
				wij = tf * icf;
				
				Text outputValue = new Text();
				Text outputKey = new Text();

				outputKey.set(outputString);
				outputValue.set(String.valueOf(wij));

				tficfMap.put(outputKey, outputValue);
			}
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
