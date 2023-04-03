import java.io.*;
import java.util.*;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import org.apache.log4j.*;

public class Hw1Grp5 {
	public static String fileDir = "hdfs://localhost:9000";
	public static String outputTable = "Result";
	public static String colFamily = "res";
	
	public static String inputFile;
	public static Integer compCol;
	public static String compFunc;
	public static Double compNum;
	public static int[] outputCols;
	
	public static void main(String[] args) throws IOException, URISyntaxException{
		if (args.length != 3){
			System.out.println("Illegal Input");
			return ;
		}
		
		String[] parse = parser(args);
		
		inputFile = fileDir + parse[0];
		compCol = Integer.parseInt(parse[1]);
		compFunc = parse[2];
		compNum = Double.valueOf(parse[3]);
		String[] outputcol = parse[4].split(",");
		int outputlen = outputcol.length;
		int[] outputCols = new int[outputlen];
		for(int i=0; i<outputlen; i++){
			outputCols[i] = Integer.parseInt(outputcol[i].substring(outputcol[i].indexOf("R")+1));
		}
		
		// DEBUG
		System.out.println("inputFile: " + inputFile); // hdfs://localhost:9000/hw1-input/input/***.tbl
		System.out.println("compCol: " + compCol); // 1
		System.out.println("compFunc: " + compFunc); // gt
		System.out.println("compNum: " + compNum); // 5.1
		System.out.println("Output len: " + outputlen); // 3
		System.out.println("Output Cols: ");
		for (int q=0; q<outputlen; q++){
			System.out.println(outputCols[q]); // 2 3 5
		}
		
		// Read HDFS
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(inputFile), conf);
		FSDataInputStream in_stream = fs.open(new Path(inputFile));
		
		BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
		String inputline;
		
		// select
		ArrayList<String> outputList = new ArrayList<String>();
		while ((inputline=in.readLine()) != null) {
			String[] inputLine = inputline.split("\\|");
			Double x = Double.valueOf(inputLine[compCol]);
			
			// DEBUG
			System.out.println("inputline: " + inputline);
			System.out.println("compCol: " + compCol);
			
			if(select(x)){
				String outputLine="";
				
				for(int i=0; i<outputlen; i++){
					outputLine = outputLine + inputLine[outputCols[i]] + "|";
				}
				System.out.println("outputLine: " + outputLine);
				outputList.add(outputLine);
			}
		}
		
		in.close();
		fs.close();
		// sort
		
		// distinct
		
		// Write HBase
		
		return ;
	}
	
	public static String[] parser(String[] args){
		// legal command:
		// java Hw1Grp5 R=<file> select:R1,gt,5.1 distinct:R2,R3,R5
		// input_file: <file>
		// comp_col: 1, comp_func: gt, comp_num: 5.1
		// output_cols: R2,R3,R5
		
		// parse = [input_file, comp_col, comp_func, comp_num, output_cols]
		String[] parse = new String[5];
		
		String input_file = args[0].substring(2);
		
		String comp_col = args[1].substring(args[1].indexOf("R")+1,args[1].indexOf(","));
		String comp_func = args[1].split(":")[1].split(",")[1];
		String comp_num = args[1].split(":")[1].split(",")[2];
		
		String output_cols = args[2].split(":")[1];
		
		parse[0] = input_file;
		parse[1] = comp_col;
		parse[2] = comp_func;
		parse[3] = comp_num;
		parse[4] = output_cols;
		
		return parse;
	}
	// compare x with compNum
	public static boolean select(double x){
		if (compFunc.equals("gt"))
			return (x > compNum);
		
		if (compFunc.equals("ge"))
			return (x >= compNum);
		
		if (compFunc.equals("eq"))
			return (x == compNum);
		
		if (compFunc.equals("ne"))
			return (x != compNum);
		
		if (compFunc.equals("le"))
			return (x <= compNum);
		
		if (compFunc.equals("lt"))
			return (x < compNum);
		
		return false;
	}
}