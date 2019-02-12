import java.io.*;
import java.nio.file.Files;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Application {

	static int K = 10;
	static String ifile = "datapoints.txt";
	public static void main(String[] args) throws Exception
	{
		//gen_points(); //used in initial runs to create a dataset as per the assignment instructions
		//TODO: ADD ITERATIONS
		 Configuration conf = new Configuration();
		    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		    if (otherArgs.length != 3) {
		      System.err.println("Usage: <in> <out> <centers>\n");
		      System.exit(2);
		    }

		    int iteration = 0;    
		    Job job = new Job(conf, "kmeans");
		    Path toCache = new Path(otherArgs[2]);
		    job.addCacheFile(toCache.toUri());
		    job.createSymlink();
				     
		    job.setJarByClass(Application.class);
		    job.setMapperClass(Mapper.class);
		    job.setReducerClass(Reducer.class);

		    job.setInputFormatClass(TwoDPointFileInputFormat.class);
		    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

		    job.setMapOutputKeyClass(IntWritable.class);
		    job.setMapOutputValueClass(TwoDPointWritable.class);

		    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		    job.setOutputKeyClass(IntWritable.class);
		    job.setOutputValueClass(Text.class);

		    System.out.println(">>>>>\tRunning iteration 0\t<<<<<");
			System.out.println(">>>>>\tCenters file: " + otherArgs[2]);
		    job.waitForCompletion(true);
		    
		    String centers = rewriteCenters(otherArgs[1], otherArgs[2], iteration);
			
		    boolean loop = true;
		    if(converged(otherArgs[1] + "/part-r-00000", otherArgs[2]))
		    {
		    	loop = false;
		    }
		    else
		    {
		    	iteration++;
		    }
			while (loop) 
			{  
				System.out.println(">>>>>\tRunning iteration " + iteration + "\t<<<<<");
				System.out.println(">>>>>\tCenters file: " + centers);
				// for iterations 1, 2, 3, … 
				conf= new Configuration(); 
				conf.set("centroid.path", centers); 
				conf.set("num.iteration", iteration + ""); 
				job = Job.getInstance(conf); 
				job.setJobName("KMeansClustering " + iteration); 
				job.setMapperClass(Mapper.class); 
				job.setReducerClass(Reducer.class); 
				job.setJarByClass(Application.class);
				
				//Path in= new Path("files/clustering/depth_" + (iteration -1) + "/");
				Path in = new Path(otherArgs[0]);
				//actually make the new output path before you set it
				new File(otherArgs[1] + Integer.toString(iteration)).mkdir();
				Path out= new Path(otherArgs[1] + iteration); 
				FileInputFormat.addInputPath(job, in); 
				// previous output-path as input
				File fs = new File(out.toString());
				if (fs.exists())      fs.delete();
				
				FileOutputFormat.setOutputPath(job, out); 
				job.setInputFormatClass(TwoDPointFileInputFormat.class); 
				//job.setOutputFormatClass(??OutputFormat.class); 
				job.setOutputKeyClass(IntWritable.class); 
				job.setOutputValueClass(Text.class);
		
				job.waitForCompletion(true); 
				iteration++; 
				if (converged(out.toString(), centers)) break;
				centers = rewriteCenters(out.toString(), otherArgs[2], iteration);
		}
			System.out.println("Converged to within 0.1 on iteration " + iteration);
	}
	
	public static boolean converged(String a, String b)
	{
		boolean ret = true;
		try {
			Scanner sa = new Scanner(new FileReader(a));
			Scanner sb = new Scanner(new FileReader(b));
			
			for(int i = 0; i < K; i++)
			{
				System.out.println(">>>>>\tTesting convergence " + i + " ...");
				int pa = sa.nextInt();
				int pb = sa.nextInt();
				
				float pa1 = sa.nextFloat();
				float pb1 = sb.nextFloat();
				float pa2 = sa.nextFloat();
				float pb2 = sb.nextFloat();
				
				if(!(pa == pb && Math.abs(pa1-pb1) < 0.1 && Math.abs(pa2-pb2) < 0.1))
				{
					//if at any point the points aren't close enough, return that it has not converged
					sa.close();
					sb.close();
					return false;
				}
			}
			sa.close();
			sb.close();
			return ret;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}
	
	
	public static String rewriteCenters(String p, String c, int i)
	{
		try {
			// the last output file contains the new centers
			p = p + "/part-r-00000";
			Scanner sp = new Scanner(new FileReader(p));
			//construct the path to a new centers file
			String[] cs = c.split(".");
			c = cs[0] + "_new." + cs[1];
			File f = new File(c);
			//delete the old centers_new file if it exists
			if (f.exists())      f.delete();
			f.createNewFile();
			FileWriter o = new FileWriter(f);
			//read the new centers from the old output
			String s = "";
			for(int x = 0; x < K; x++)
			{
				int pos = sp.nextInt();
				float a = sp.nextFloat();
				float b = sp.nextFloat();
				s+= String.format("%d %f %f\n", pos, a, b);
			}
			sp.close();
			//put them into the new file
			o.write(s);
			o.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//if we get here with no exceptions, we have the new file
		//if we get here after an exception, we just go back to the old centers
		return c;
	}
	
	public static void gen_points()
	{
		try {
			FileWriter f = new FileWriter(ifile);
		
			int control = 1001;
			String s = "";
			for(int i = 0; i < control; i++)
			{
				double a = Math.random() * 10000;
				double b = Math.random() * 10000;
				s += String.format("%.2f %.2f\n", a, b);
						//Double.toString(a) + " " + Double.toString(b) + "\n";
			}
			//System.out.println(s);
			f.write(s);
			f.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
