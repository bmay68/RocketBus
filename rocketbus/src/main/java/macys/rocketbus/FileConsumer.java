package macys.rocketbus;

import java.io.File;
import java.nio.file.Path;
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
	
	public void Run(Path path) {
		_log.debug("Run");
		
		// Check path for null
		if (path == null) {
			_log.warn("null path used");
			return;
		}
		
		// We have to build up the path + filename based on using directory watcher
		String fullPath = App.WatchPath + File.separator + path.getFileName();
		_log.debug("Process file {}", fullPath);

		// Check to see if files are located in a directory for processing
		File f = new File(fullPath);
		if (f.exists()) {
			// TODO parse records
			
			// TODO Place records onto ConcurrentLinkedQueue
			
			// TODO Need to add records onto linked queue not the object
			_q.add(new Object());
		} else {
			_log.warn("File not found {}", fullPath);
		}
	}
}
