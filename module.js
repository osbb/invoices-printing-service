import { getRabbitConnection } from './rabbit-connection';
import winston from 'winston';
import uuid from 'uuid';
import PDFDocument from 'pdfkit';
import { S3 } from 'aws-sdk';
import { WriteStream } from 's3-streams';

function sendResponseToMsg(ch, msg, data) {
  return ch.sendToQueue(
    msg.properties.replyTo,
    new Buffer(JSON.stringify(data)),
    { correlationId: msg.properties.correlationId }
  );
}

// wait for connection to RabbitMQ and MongoDB
getRabbitConnection()
// create channel rabbit
  .then(conn => conn.createChannel())
  .then(ch => {
    // create topic
    ch.assertExchange('events', 'topic', { durable: true });
    // create queue
    ch.assertQueue('invoices-printing-service', { durable: true })
      .then(q => {
        // fetch by one message from queue
        ch.prefetch(1);
        // bind queue to topic
        ch.bindQueue(q.queue, 'events', 'print.invoice');
        // listen to new messages
        ch.consume(q.queue, msg => {
          let data;

          try {
            // messages always should be JSONs
            data = JSON.parse(msg.content.toString());
          } catch (err) {
            // log error and exit
            winston.error(err, msg.content.toString());
            return;
          }

          const s3 = new S3();
          const Key = `${uuid.v4()}.pdf`;
          const Bucket = 'invoices-osbb';
          const upload = WriteStream(s3, {
            Bucket,
            Key,
            ContentType: 'application/pdf',
            ACL: 'public-read',
          });
          const doc = new PDFDocument({
            size: 'A5',
            layout: 'landscape',
          });
          doc.pipe(upload);

          doc.text(`Hello ${data.firstName}!`);

          doc.end();

          s3.getSignedUrl('getObject', {
            Bucket,
            Key,
          }, (err, url) => {
            Promise.resolve(sendResponseToMsg(ch, msg, { url }))
              .then(() => ch.ack(msg));
          });
        });
      });
  });
