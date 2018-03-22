import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
public class WriterThread implements Runnable {
	String fileName;
	private final String DECIMAL_REGEX="[-+]?[0-9]*\\.?[0-9]+";
	long threadHashCode;
	String sql;
	HashMap<String,Integer> header;
	HashMap<String,Integer> tableMetaData;
	HashMap<String,HashMap<String,String>> tableMappingDesc;
	HashMap<Long, int[]> threadStatus;
	int start,end;
	final Logger logger = Logger.getLogger("Global Logger");
	private final int BATCH_SIZE=50;
	private synchronized void errorinfo(Connection con,String error,int rownum)
	{
		PreparedStatement ps=null;
		try {
			ps=con.prepareStatement("insert into errortable values(?,?)");
			ps.setInt(1, rownum);
			ps.setString(2, error);
			if(ps.executeUpdate()==1)
			{
				logger.info("Error Msg Inserted");
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			logger.error(e1.toString());
		}
		
	}
	private synchronized void serialize()
	{
		FileOutputStream fileOut;
		try {
		
			fileOut = new FileOutputStream("ser_files/write_record.ser");
			 ObjectOutputStream out = new ObjectOutputStream(fileOut);
			 out.writeObject(threadStatus);
			 out.close();
			 fileOut.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace(); 	
		}
	}
	public void run()
	{  
		Connection con=null;
		CSVReader csvReader=null;
		PreparedStatement ps=null;
		FileInputStream input;
		long i=0;
		long current=start-1;
		try {
			input = new FileInputStream(fileName);
			CharsetDecoder decoder=Charset.forName("UTF-8").newDecoder();
			decoder.onMalformedInput(CodingErrorAction.IGNORE);
			Reader reader=new InputStreamReader(input,decoder);		 		    
			csvReader =new CSVReaderBuilder(reader).withSkipLines(start).build();  		 		     
			con=C3P0DataSource.getInstance().getConnection();
			con.setAutoCommit(false);
			for(i=start;i<=end;i++)
			{
				String[] data=csvReader.readNext();
				if((i-start+1)%BATCH_SIZE==1)
				{
					ps=con.prepareStatement(sql);
				}
				
				for(Map.Entry<String, HashMap<String,String>> mapEntry:tableMappingDesc.entrySet())
		    	{
		    		HashMap<String,String> csvHeader=mapEntry.getValue();
		    		if(csvHeader.size()==1)
		    		{
		    			for(Map.Entry<String,String> entry:csvHeader.entrySet())
			    		{
		    				String dataType=entry.getValue();
		    				System.out.println(dataType);
		    				String[] precision=null;
		    				if(entry.getValue().contains("Decimal"))
		    				{		
		    					System.out.println(dataType.substring(dataType.indexOf("(")+1, dataType.length()-1));
		    					precision=dataType.substring(dataType.indexOf("(")+1, dataType.length()-1).split(",");
    							dataType="Decimal";
		    				}
		    				
			    			DbDataTypeEnum var=DbDataTypeEnum.valueOf(dataType);
			    			switch(var)
			    			{
			    			case Decimal:
			    				if(Pattern.matches(DECIMAL_REGEX, data[header.get(entry.getKey())]))
			    				{
			    					ps.setBigDecimal(tableMetaData.get(mapEntry.getKey()), new BigDecimal(data[header.get(entry.getKey())]).setScale(Integer.parseInt(precision[1]),RoundingMode.HALF_EVEN));
			    				}
			    				else
			    				{
			    					errorinfo(con,"Cannot parse String to Decimal",(int) i);
			    				}
			    				break;
			    			case String:
			    				ps.setString(tableMetaData.get(mapEntry.getKey()), data[header.get(entry.getKey())]);
			    				break;
			    			case File:
			    				File dataFile=new File(data[header.get(entry.getKey())]);
			    				if(dataFile.exists())
			    				{
			    					FileInputStream fis=new FileInputStream(dataFile);
				    				ps.setBinaryStream(tableMetaData.get(mapEntry.getKey()), fis,dataFile.length());
			    				}
			    				else
			    				{
			    					errorinfo(con,"Invalid File Path",(int) i);
			    				}
			    				break;
			    			case Boolean:
			    			
			    				ps.setInt(tableMetaData.get(mapEntry.getKey()),(Boolean.parseBoolean(data[header.get(entry.getKey())])?1:0 ));
			    				break;
			    			default:
			    				errorinfo(con,"Invalid DataType",(int) i);
			    			}
			    		}
		    		}
		    	}
				ps.addBatch();
				if((i-start+1)%BATCH_SIZE==0)
				{
					int[] update=ps.executeBatch();
					current+=update.length;
					for(int k=0;k<update.length;k++)
					{
						logger.info("/****************************************Processed  " +(i-BATCH_SIZE+k+1)+ "th Record********************************************************************************/");	
					}
					con.commit();
					int[] recordStatus=threadStatus.get(threadHashCode);
					recordStatus[2]=(int) current;
					threadStatus.put(threadHashCode, recordStatus);
					serialize();
					ps.close();
				}
    		}
			int[] update=ps.executeBatch();
			current+=update.length;
			for(int k=0;k<update.length;k++)
			{
				logger.info("/****************************************Processed  " +(i-update.length+k+1)+ "th Record********************************************************************************/");
			}
			con.commit();
			int[] recordStatus=threadStatus.get(threadHashCode);
			recordStatus[2]=(int) current;
			threadStatus.put(threadHashCode, recordStatus);
			serialize();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			errorinfo(con,e.getMessage(),(int) i);
    		logger.error(e.toString());
		}
		catch (BatchUpdateException buex) {
			errorinfo(con,buex.getMessage(),(int) i-BATCH_SIZE+1);
    		logger.error(buex.toString());
			try {
				con.rollback();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		catch ( SQLException e) {
			// TODO Auto-generated catch block
			errorinfo(con,e.getMessage(),(int) i);
    		logger.error(e.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			errorinfo(con,e.getMessage(),(int) i);
    		logger.error(e.toString());
		}
		finally {
			try {
				ps.close();
				con.close();
				csvReader.close();
			} catch (SQLException | IOException e) {
				// TODO Auto-generated catch block
				errorinfo(con,e.getMessage(),(int) i);
	    		logger.error(e.toString());
			}
		}
			
	}  
	public void setIndex(int s,int e,HashMap<Long, int[]> threadStatus)
	{
		this.threadStatus=threadStatus;
		start=s;
		end=e;
	}
	WriterThread(String file,String sql,HashMap<String,Integer> header,HashMap<String,HashMap<String,String>> tableMappingDesc,HashMap<String,Integer> tableMetaData)
	{
		this.tableMetaData=tableMetaData;
		this.tableMappingDesc=tableMappingDesc;
		this.header=header;
		threadHashCode=this.hashCode();
		fileName=file;
		this.sql=sql;
	}
}
