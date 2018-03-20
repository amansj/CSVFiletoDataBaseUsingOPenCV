import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectOutputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
public class WriterThread implements Runnable {
	String fileName;
	long threadHashCode;
	String sql;
	HashMap<String,Integer> header;
	HashMap<String,Integer> tableMetaData;
	HashMap<String,HashMap<String,String>> tableMappingDesc;
	HashMap<Long, int[]> threadStatus;
	int start,end;
	final Logger logger = Logger.getLogger("Global Logger");
	

	private synchronized void errorinfo(Connection con,Exception e,int rownum)
	{
		PreparedStatement ps=null;
		try {
			ps=con.prepareStatement("insert into errortable values(?,?)");
			ps.setInt(1, rownum);
			ps.setString(2, e.getMessage());
			if(ps.executeUpdate()==1)
			{
				logger.info("Error Msg Inserted");
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			logger.error(e.toString());
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
 		try {
 			FileInputStream input=new FileInputStream(fileName);
 			CharsetDecoder decoder=Charset.forName("UTF-8").newDecoder();
 			decoder.onMalformedInput(CodingErrorAction.IGNORE);
 			Reader reader=new InputStreamReader(input,decoder);		 		    
 			csvReader =new CSVReaderBuilder(reader).withSkipLines(start).build();  		 		     
 			con=C3P0DataSource.getInstance().getConnection();
 			PreparedStatement ps=null;
 			for(long i=start;i<=end;i++)
 			{  
 				Object[] data=csvReader.readNext();
			    try 
			    {
			    	ps=con.prepareStatement(sql);
			    	//ps.setObject(parameterIndex, x);
			    	for(Map.Entry<String, HashMap<String,String>> mapEntry:tableMappingDesc.entrySet())
			    	{
			    		
			    		HashMap<String,String> csvHeader=mapEntry.getValue();
			    		for(Map.Entry<String,String> entry:csvHeader.entrySet())
			    		{
			    			DbDataTypeEnum var=DbDataTypeEnum.valueOf(entry.getValue());
			    			if(var.getter().equals(BigDecimal.class))
			    			{
			    				ps.setBigDecimal(tableMetaData.get(mapEntry.getKey()), new BigDecimal((String)data[header.get(entry.getKey())]));
			    			}
			    			else
			    			{
			    				ps.setObject(tableMetaData.get(mapEntry.getKey()), var.getter().cast(data[header.get(entry.getKey())]));
			    			}
			    			
			    		}
			    	}
				    int r=ps.executeUpdate();
				   
				    if(r==1) {
				    	int[] recordStatus=threadStatus.get(threadHashCode);
						recordStatus[2]+=1;
					    logger.info("/****************************************Processed  " +i+ "th Record********************************************************************************/");
			    	}
			    }
			    catch (SQLException e) {
					// TODO Auto-generated catch block
			    	errorinfo(con,e,(int) i);
			    	logger.error(e.toString());
				}
			    finally {
			    	try {
					   if(ps!=null)
					   {
						   ps.close();
						   serialize();
					   }
					} 
			    	catch (SQLException e) {
						// TODO Auto-generated catch block
			    		errorinfo(con,e,(int) i);
			    		logger.error(e.toString());
					}
			    	
   				} 
			}
	   	}catch (Exception e) {
			// TODO Auto-generated catch block
			errorinfo(con,e,start);
			logger.error(e.toString());
   		}
 		finally 
 		{
 			try {
 				if(con!=null)
 				{
 					con.commit();
 					con.close();
 				}
 				if(csvReader!=null)
 				{
 					csvReader.close();
 				}
 			} catch (SQLException e) {
 				// TODO Auto-generated catch block
 				errorinfo(con,e,start);
 				logger.error(e.toString());
 			} catch (IOException e) {
 				// TODO Auto-generated catch block
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
