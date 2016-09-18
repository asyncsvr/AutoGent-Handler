package util;
import java.sql.Connection;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import main.AutoGentHandler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
 
public class CollectUtil implements Runnable {
	private static final Logger LOG = LogManager.getLogger(AutoGentHandler.class);
    private KafkaStream m_stream;
    private Conf cf;
    private int m_threadNumber;
 
    public CollectUtil(KafkaStream a_stream, int a_threadNumber,Conf cf) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        this.cf=cf;
    }
 
    public void run() {
    	System.out.println("hi");
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();

		RDao rDao=null;
		Connection con=null;
		String dbType=cf.getSingleString("main_db_type");
        if (dbType.matches("PostgreSQL")){
        	rDao=new RDao();
    		rDao.setRdbPasswd(cf.getSingleString("password"));
    		rDao.setRdbUrlNdbType(cf.getDbURL());
    		rDao.setRdbUser(cf.getSingleString("user"));
    		con=rDao.getConnection();
        }	
        if (dbType.matches("Oracle")){
        	rDao=new RDao();
    		rDao.setRdbPasswd(cf.getSingleString("password"));
    		rDao.setRdbUrlNdbType(cf.getDbURL());
    		rDao.setRdbUser(cf.getSingleString("user"));
    		con=rDao.getConnection();
        }
        if (dbType.matches("Cassandra")){
        	
        }
        if (dbType.matches("HDFS")){
        	
        }
        	
        while (it.hasNext()){
        	String message=new String(it.next().message());
        	LOG.info("Thread " + m_threadNumber + ": " +message);
        	
    		String[] items =message.split("::");
    		try{
				//String sendMsg = revAuthCode+"::"+resRcsvr+"::"+revMode+"::"+revCmdType+"::"+cmd;
				String jobID=items[0];//
				String hostname=items[1];//norm/lock
				String[] resItems=items[2].split("<:-:>"); 
				String result=resItems[1];//norm/lock
				String status=resItems[0];
				rDao.setJobResult(con, result,Integer.parseInt(jobID), hostname,dbType,status);
    		}catch (ArrayIndexOutOfBoundsException e){
    			LOG.info("Array Out");
    		}catch (NumberFormatException e){
    			LOG.error("NumberFormatException:"+message);
    		}  		
        }
        LOG.info("Shutting down Thread: " + m_threadNumber);
    }
}