package macys.rocketbus;

import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import macys.rocketbus.FileConsumer;
import macys.rocketbus.RocketProducer;

public class App
{
	// TODO: Move to rocketbus.properites file
	public static final String WatchPath = "/tmp/Test";
	private static final int _watchPollTimeout = 10000;
	
	// Logger for execution visibility
	private static final Logger _log = LogManager.getLogger(App.class);
	
	// A shared sync'd buffer between consuming files and producing messages
	private ConcurrentLinkedQueue<Object> _q = new ConcurrentLinkedQueue<Object>();
	
	private FileConsumer _fileConsumer = null;
	
	private RocketProducer _rocketProducer = null;
	
	private WatchService _watcher = null;

	/**
	 * Initialize the application by creating core classes that handle units of work.
	 * 
	 * @throws NullPointerException
	 */
	public App() throws NullPointerException, InvalidPathException, Exception {
		_fileConsumer = new FileConsumer(_q);
		_rocketProducer = new RocketProducer(_q);
		
		Path path = Paths.get(WatchPath);
		_watcher = path.getFileSystem().newWatchService();
		path.register(_watcher, StandardWatchEventKinds.ENTRY_CREATE);
	}
	
	/**
	 * This kicks off and maintains execution of FileConsumer/RocketProducer objects
	 */
	public void Run() {
		_log.debug("Run");
				
		while (true) {
			try {
				WatchKey watchKey = _watcher.poll();
				if (watchKey == null)
				{ 
					Thread.sleep(_watchPollTimeout);
					continue;
				}
				
				// Get the list of change events
				List<WatchEvent<?>> events = watchKey.pollEvents();
				for (@SuppressWarnings("rawtypes") WatchEvent evt : events) {
					if (evt.kind() == StandardWatchEventKinds.ENTRY_CREATE) {
						Path path = (Path) evt.context();
						_fileConsumer.Run(path);
						_rocketProducer.Run();
					}
				}
				watchKey.reset();
			} catch (InterruptedException e) {
				_log.error(e.toString());
				continue;
			} catch (Exception e) {
				_log.error(e.toString());
				continue;
			}
		}
	}
	
    public static void main( String[] args )
    {
    	try {
        	App _app = new App();
        	_app.Run();
    	} catch(NullPointerException e) {
    		_log.fatal("Unable to initialize application {}", e.toString());
    	} catch (InvalidPathException e) {
    		_log.fatal("Unable to initialize application {}", e.toString());
    	} catch(Exception e) {
    		_log.fatal("Unable to initialize application {}", e.toString());
    	}
    }
}
