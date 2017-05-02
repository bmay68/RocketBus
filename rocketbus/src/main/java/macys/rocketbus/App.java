package macys.rocketbus;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import macys.rocketbus.FileConsumer;
import macys.rocketbus.RocketProducer;

public class App
{
	// Logger for execution visibility
	private static final Logger _log = LogManager.getLogger(App.class);
	
	// A shared sync'd buffer between consuming files and producing messages
	private ConcurrentLinkedQueue<Object> _q = new ConcurrentLinkedQueue<Object>();
	
	private FileConsumer _fileConsumer = null;
	
	private RocketProducer _rocketProducer = null; 

	/**
	 * Initialize the application by creating core classes that handle units of work.
	 * 
	 * @throws NullPointerException
	 */
	public App() throws NullPointerException {
		_fileConsumer = new FileConsumer(_q);
		_rocketProducer = new RocketProducer(_q);
	}
	
	/**
	 * This kicks off and maintains execution of FileConsumer/RocketProducer objects
	 */
	public void Run() {
		_log.debug("Run");
		// TODO Setup observer pattern and run FileConsumer and RocketProducer as threads or jobs

		// TODO Setup quartz to run the jobs
		_fileConsumer.Run();
		_rocketProducer.Run();
	}
	
    public static void main( String[] args )
    {
    	try {
        	App _app = new App();
        	_app.Run();
    	} catch(NullPointerException e) {
    		_log.fatal("Unable to initialize application {}", e.toString());
    	}
    }
}
