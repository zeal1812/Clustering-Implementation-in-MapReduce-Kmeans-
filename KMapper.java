import java.io.IOException;
import java.net.URI;
import java.util.Scanner;
import java.io.FileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

public class KMapper extends Mapper<LongWritable, TwoDPointWritable, IntWritable, TwoDPointWritable>{
    
	//public final static String centerfile="centers.txt";
    public int K = 10; //hardcoded to K=10 as in the directions
    public float[][] centroids = new float[K][2];
    
    public void setup(Context context) throws IOException {
	  //Scanner reader = new Scanner(new FileReader(centerfile));
    	Configuration conf = context.getConfiguration();
    	URI[] files = Job.getInstance(conf).getCacheFiles();
    	Path p = new Path(files[0]);
    	String cfile = p.getName();
    	Scanner reader = new Scanner(new FileReader(cfile));
	  for (int  i=0; i<K; i++ ) {	
	      int pos = reader.nextInt();

	      centroids[pos][0] = reader.nextFloat();
	      centroids[pos][1] = reader.nextFloat();
	  }
	  reader.close();
    }

    public void map(LongWritable key, TwoDPointWritable value, Context context
	  ) throws IOException, InterruptedException {

	  // Read number of centroids and current centroids from file
	  // calculate distance of given point to each Centroid
	  // winnerCentroid = centroid with minimarl distance for this point
	  float distance=0;
	  float mindistance=999999999.9f;
	  int winnercentroid=-1;
	  int i=0;
	  for ( i=0; i<K; i++ ) {
	      float x = value.getx();
	      float y = value.gety();
	      distance = ( x-centroids[i][0])*(x-centroids[i][0]) + 
		  (y - centroids[i][1])*(y-centroids[i][1]);
	      if ( distance < mindistance ) {
		  mindistance = distance;
		  winnercentroid=i;
	      }
	  }

	  IntWritable winnerCentroid = new IntWritable(winnercentroid);
	  context.write(winnerCentroid, value);
	  System.out.printf("Map: Centroid = %d distance = %f\n", winnercentroid, mindistance);
    }

}
