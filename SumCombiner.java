import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private int wordCount = 0;
	private IntWritable intWritable = new IntWritable();
	
	
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {

	  
		for (IntWritable value : values) {

				wordCount += value.get();
			}
	
		// Setting our writable to wordCount
		intWritable.set(wordCount);
		
		
		// writing the output.
		context.write(key, intWritable);  

  }
}