package macys.rocketbus;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
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
			// Check file to see if there is any data to read
			if (f.length() < 1) { return; }
			
			// Read the file into memory for processing
			String s = readFileToString(f);
			if (s == null) { return; }
			
			// Parse records
			String recs[] = s.split("\\r?\\n");
			for (String r : recs) {
				// Place records onto ConcurrentLinkedQueue
				_q.add(r);
			}
		} else {
			_log.warn("File not found {}", fullPath);
		}
	}
	
	/**
	 * Given a valid file attempt to read the contents into a string. Since we don't limit
	 * the size in any shape or form this method may be dangerous for extremely large files.
	 * @param f - an initialized and valid File object from which to read contents
	 * @return String on success or null on error
	 */
	private String readFileToString(File f) {
		FileInputStream fis;
		try {
			fis = new FileInputStream(f);
			byte[] data = new byte[(int) f.length()];
			fis.read(data);
			fis.close();
			String str = new String(data, "UTF-8");
			return str;
		} catch (FileNotFoundException e) {
			_log.warn(e.toString());
			return null;
		}
		catch (UnsupportedEncodingException e) {
			_log.error(e.toString());
			return null;
		}		
		catch (IOException e) {
			_log.error(e.toString());
			return null;
		}
	}
}
