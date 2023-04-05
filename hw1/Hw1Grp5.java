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
	public static int outputlen;
	
	public static void main(String[] args) throws IOException, URISyntaxException, MasterNotRunningException, ZooKeeperConnectionException{
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
		outputlen = outputcol.length;
		int[] outputCols = new int[outputlen];
		for(int i=0; i<outputlen; i++){
			outputCols[i] = Integer.parseInt(outputcol[i].substring(outputcol[i].indexOf("R")+1));
		}
		
		// Read HDFS
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(inputFile), conf);
		FSDataInputStream in_stream = fs.open(new Path(inputFile));
		BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
		
		// select
		String inputline = "";
		String outputline = "";
		ArrayList<String> outputList = new ArrayList<String>();
		while ((inputline=in.readLine()) != null) {
			outputline = select(inputline, outputCols);
			if (outputline != "")
				outputList.add(outputline);
		}
		
		in.close();
		fs.close();
		
		// sort and distinct
		outputList = sort_distinct(outputList);
		
		// Create HBase Table
		Logger.getRootLogger().setLevel(Level.WARN); 
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(outputTable));
		HColumnDescriptor cf = new HColumnDescriptor(colFamily);
		htd.addFamily(cf);
		Configuration configuration = HBaseConfiguration.create();
		HBaseAdmin hAdmin = new HBaseAdmin(configuration);
		
		if (hAdmin.tableExists(outputTable)) {
			System.out.println("Table already exists");
			hAdmin.disableTable(outputTable);
			hAdmin.deleteTable(outputTable);
			System.out.println("Delete Table success");
		}
		
		hAdmin.createTable(htd);
		hAdmin.close();
		System.out.println("Table " + outputTable + " created successfully");
		
		// Write HBase Table
		HTable table = new HTable(configuration,outputTable);
		int rowNo = 0;
		for(int i=0; i<outputList.size(); i++){
			Put put = new Put((rowNo+"").getBytes());
			outputline = outputList.get(i);
			String[] outputLine = outputline.split("\\|");
			
			for(int j=0; j<outputLine.length; ++j){
				put.add(colFamily.getBytes(), ("R"+outputCols[j]).getBytes(), outputLine[j].getBytes());
			}
			
			table.put(put);
			rowNo++;
		}
		table.close();
		System.out.println("Write success");
		return ;
	}
	
	public static String[] parser(String[] args){
		// legal command:
		// java Hw1Grp5 R=/input/distinct_0.tbl select:R1,gt,1.1 distinct:R1
		// input_file: /input/distinct_0.tbl
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
	
	public static String select(String ipline, int[] opCols){
		String[] ipLine = ipline.split("\\|");
		Double x = Double.valueOf(ipLine[compCol]);
		
		String opLine = "";
		if(selectx(x)){
			for(int i=0; i<outputlen; i++){
				opLine = opLine + ipLine[opCols[i]] + "|";
			}
		}
		
		return opLine;
	}
	
	// compare x with compNum
	public static boolean selectx(double x){
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
	
	public static ArrayList<String> sort_distinct(ArrayList<String> opList){
		// sort
		Object[] opSort = opList.toArray();
		Arrays.sort(opSort);
		
		// distinct
		String opstr_i = opSort[0].toString();
		String opstr_i_1 = "";
		opList.clear();
		opList.add(opstr_i);
		
		for(int i=1; i<opSort.length; i++){
			opstr_i_1 = opstr_i; // = opSort[i-1].toString()
			opstr_i = opSort[i].toString();
			
			if (!opstr_i.equals(opstr_i_1)){
				opList.add(opstr_i);
			}
		}
		
		return opList;
	}
}