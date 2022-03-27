import boto3
import logging
import json

logging.getLogger().setLevel(logging.logProcesses)


def main():
    while True:
        # Create client
        client = boto3.client(
            service_name='sqs',
            endpoint_url='https://message-queue.api.cloud.yandex.net',
            region_name='ru-central1'
            )
        # base_queue_url = 'https://message-queue.api.cloud.yandex.net/b1g78qvrk6t50ij57gni/dj6000000005ihjg04tj/base_queue'

        # Receive messages from base queue
        print(1)
        base_queue_url = client.create_queue(QueueName='base_queue').get('QueueUrl')
        messages = client.receive_message(
            QueueUrl=base_queue_url,
            MaxNumberOfMessages=10,
            VisibilityTimeout=60,
            WaitTimeSeconds=20
        ).get('Messages')

        if isinstance(messages, list):
            for msg in messages:
                received_msg = json.loads(msg.get('Body').replace("'", '"'))
                print('Received message: "{}"'.format(msg.get('Body')))

                # body = event['body']
                #    a = int(body.get("a"))
                #    return {
                #        'body':event['body'],
                #        'summ':a+b
                #    }
                temporary_queue_url = received_msg.get('created_queue')
                # Send message to temporary queue
                body_text = {'message': 'Success!!!', 'created_queue': temporary_queue_url, 'is_answer': "True"}
                client.send_message(
                    QueueUrl=temporary_queue_url,
                    MessageBody=str(body_text)
                )
                print('Successfully sent message to temporary queue')
            # # Delete queue
            # client.delete_queue(QueueUrl=queue_url)
            # print('Successfully deleted temporary queue')


if __name__ == '__main__':
    main()






