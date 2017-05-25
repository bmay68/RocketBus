package macys.rocketbus;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MQConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

public class RocketConsumer {
	
	private static final Logger _log = LogManager.getLogger(RocketProducer.class);
	
	// TODO: Move this into properties file
	private static final String _groupName = "MY_GROUP_NAME";
		
	private static final String _topic = "SELF_TEST_TOPIC";
		
	private static final String _tag = "MY_TAG";
	
	private static final String _nameServer = "192.168.177.129:9876";
	
	private MQConsumer _consumer;
	
	public RocketConsumer() throws Exception {
		_log.debug("RocketConsumer()");
		
		_consumer = new DefaultMQPushConsumer(_groupName);
		((DefaultMQPushConsumer) _consumer).setNamesrvAddr(_nameServer);
		
		try {
			((DefaultMQPushConsumer)_consumer).subscribe(_topic, _tag);
			((DefaultMQPushConsumer)_consumer).registerMessageListener(new MessageListenerConcurrently() 
				{ public ConsumeConcurrentlyStatus consumeMessage(final List<MessageExt> msgs,
		            final ConsumeConcurrentlyContext context) {
				
					_log.debug("Received {} message", msgs.size());
					String s;
					for (MessageExt m : msgs) {
						try {
							s = new String(m.getBody(), "UTF-8");
							_log.info("{}", s);
						} catch (UnsupportedEncodingException e) {
							_log.error("Could not convert bytes to string");
							continue;
						}
					}
				
					return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
				}
			});
			((DefaultMQPushConsumer)_consumer).start();
		} catch (MQClientException e) {
			_log.error(e.toString());
			throw new Exception("Failed to configure and start consumer");
		}
	}
}
