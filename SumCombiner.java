import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

   
	  int wordCount = 0;
	  
		for (IntWritable value : values) {
			  
			  /*
			   * Add the value to the word count counter for this key.
			   */
				wordCount += value.get();
			}
			
			/*
			 * Call the write method on the Context object to emit a key
			 * and a value from the reduce method. 
			 */
			context.write(key, new IntWritable(wordCount));  
	  

  }
}