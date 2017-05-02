package macys.rocketbus;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RocketProducer {
	
	private static final Logger _log = LogManager.getLogger(RocketProducer.class);
	
	private ConcurrentLinkedQueue<Object> _q;
	
	public RocketProducer(ConcurrentLinkedQueue<Object> q) throws NullPointerException {
		_log.debug("RocketProducer");
		
		if (q == null) { throw new NullPointerException("q"); }
		_q = q;
	}
	
	public void Run() {
		_log.debug("Run");
		
		Object o;
		while(_q.size() > 0) {
			o = _q.poll();
			if (o == null) { continue; }
			
			// TODO Encode pay-load
			
			// TODO Build message
			
			// TODO Place message on queue
		}
	}
}
