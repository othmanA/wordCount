import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	
	IntWritable intWritable = new IntWritable();
	Text text = new Text();
	
	
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * Convert the line, which is received as a Text object,
     * to a String object.
     */
    String line = value.toString();
    
    
    for (String word : line.split("\\W+")) {
      if (word.length() > 0) {
        
    	  text.set(word);
        /*
         * Call the write method on the Context object to emit a key
         * and a value from the map method.
         */
        context.write(text, intWritable);
      }
    }
  }
}