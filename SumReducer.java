import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	// define a global intWritable instead of using new many times.
	IntWritable intWritable = new IntWritable(); 

  @Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int wordCount = 0;
		
		/*
		 * For each value in the set of values passed to us by the mapper:
		 */
		for (IntWritable value : values) {
			wordCount += value.get();
		}
		
		// Set the intWritable value.
		intWritable.set(wordCount);
		
		/*
		 * Call the write method on the Context object to emit a key
		 * and a value from the reduce method. 
		 */
		context.write(key, intWritable);
	}
}