package edu.csd.queue;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;

public class RabbitMQInstance {
	private final String schedulerQueue = "schedulerqueue";
	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
	private final String HOST = "localhost";
	private String returnMessage;

	public RabbitMQInstance() {
		factory = new ConnectionFactory();
		factory.setHost(HOST);
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.queueDeclare(schedulerQueue, true, false, false, null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TimeoutException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public String getMessage() {

		try {
			channel.basicQos(1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		returnMessage = "";
		final Consumer consumer = new DefaultConsumer(channel) {
			@Override
			public void handleDelivery(String consumerTag, Envelope envelope,
					AMQP.BasicProperties properties, byte[] body)
					throws IOException {
				String message = new String(body, "UTF-8");

				try {
					returnMessage = message;
				} finally {
					channel.basicAck(envelope.getDeliveryTag(), false);
				}
			}
		};
		try {
			channel.basicConsume(schedulerQueue, false, consumer);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return returnMessage;
	}

	private static void doWork(String task) {
		for (char ch : task.toCharArray()) {
			if (ch == '.') {
				try {
					Thread.sleep(1000);
				} catch (InterruptedException _ignored) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	public void sendMessage(String message) {
		try {
			channel.basicPublish("", schedulerQueue,
					MessageProperties.PERSISTENT_TEXT_PLAIN, message.getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
