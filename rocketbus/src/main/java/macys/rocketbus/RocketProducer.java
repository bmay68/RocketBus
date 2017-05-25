package macys.rocketbus;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

public class RocketProducer {
	
	private static final Logger _log = LogManager.getLogger(RocketProducer.class);
	
	// TODO: Move this into properties file
	private static final String _groupName = "MY_GROUP_NAME";
	
	private static final String _topic = "SELF_TEST_TOPIC";
	
	private static final String _tag = "MY_TAG";
	
	private static final String _nameServer = "192.168.177.129:9876";
	
	private ConcurrentLinkedQueue<Object> _q;
	
	private MQProducer _producer = null;
	
	public RocketProducer(ConcurrentLinkedQueue<Object> q) throws Exception, NullPointerException {
		_log.debug("RocketProducer");
		
		if (q == null) { throw new NullPointerException("q"); }
		_q = q;
		
		_producer = new DefaultMQProducer(_groupName);
		((DefaultMQProducer) _producer).setNamesrvAddr(_nameServer);
		_producer.start();
	}
	
	public void Run() {
		_log.debug("Run");
		
		String o;
		Message msg;
		while(_q.size() > 0) {
			o = (String) _q.poll();
			if (o == null) { continue; }			
			
			// Build message
			msg = new Message(_topic, _tag, o.getBytes());
			
			// TODO Place message on queue
			try {
				SendResult rslt = _producer.send(msg);
				if (!(rslt.getSendStatus() == SendStatus.SEND_OK)) {
					_log.error("Could not send message to queue - {}", rslt.getSendStatus().toString());
				}
			} catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
				_log.error("Failed to place message onto queue");
				_log.error(e.toString());
				continue;
			}
		}
	}
}
