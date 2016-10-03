import amqp from 'amqplib';
import winston from 'winston';

export function getRabbitConnection() {
  const rabbitmqUrl = process.env.RABBITMQ_URL || 'amqp://localhost:5672';
  return new Promise(resolve => {
    function openConnection() {
      winston.info('Connecting to RabbitMQ...');
      amqp.connect(rabbitmqUrl)
        .then(conn => {
          winston.info('Connected!');
          resolve(conn);
        })
        .catch(() => {
          winston.info('Connection failure. Retry in 5 sec.');
          setTimeout(() => {
            openConnection();
          }, 5000);
        });
    }

    openConnection();
  });
}
