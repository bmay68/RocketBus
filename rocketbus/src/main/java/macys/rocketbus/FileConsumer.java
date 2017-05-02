package macys.rocketbus;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FileConsumer {

	private static final Logger _log = LogManager.getLogger(FileConsumer.class);
	
	// A queue where parsed records are placed
	private ConcurrentLinkedQueue<Object> _q;
	
	public FileConsumer(ConcurrentLinkedQueue<Object> q) throws NullPointerException {
		_log.debug("FileConsumer");
		
		if (q == null) { throw new NullPointerException("q"); }
		_q = q;
	}
	
	public void Run() {
		_log.debug("Run");
		
		// TODO Check to see if files are located in a directory for processing
		
		// TODO Read each file into memory
		
		// TODO For each file parse out records
		
		// TODO Place records onto ConcurrentLinkedQueue
		
		_q.add(new Object());
	}
}
