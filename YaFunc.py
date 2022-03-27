import boto3
import logging
import random
import string
import json


logging.getLogger().setLevel(logging.DEBUG)

def is_iterable_value(x):
    try:
        iter(x)
    except TypeError:
        return False
    else:
        return True

def main():

    event = {"body": "TESTING"}
    print(event)
    # Create client
    client = boto3.client(
        service_name='sqs',
        endpoint_url='https://message-queue.api.cloud.yandex.net',
        region_name='ru-central1'
    )

    message = event['body']
    # Create base queue and get its url
    base_queue_url = client.create_queue(QueueName='base_queue').get('QueueUrl')
    print('Created queue url is "{}"'.format(base_queue_url))




    # Create temporary queue and get its url
    random_list = [random.choice(string.ascii_lowercase + string.digits if i != 5 else string.ascii_uppercase) for i in
                   range(12)]
    random_name = (''.join(random_list))
    created_queue = client.create_queue(QueueName=random_name).get('QueueUrl')
    print((type(created_queue)))
    print('Created queue url is "{}"'.format(created_queue))

    # Send temporary queue url in message to base queue
    body_text = {"message": message, "created_queue": created_queue}
    try:
        client.send_message(
            QueueUrl=base_queue_url,
            MessageBody=str(body_text)
        )
        print('Successfully sent message to base queue')
    except:
        print('Unsuccessfully sent message to base queue')
    answer = {}
    break_the_loop = False
    # Receive sent message from temporary queue
    while True:

        print("while loop")
        messages = client.receive_message(
            QueueUrl=created_queue,
            MaxNumberOfMessages=10,
            VisibilityTimeout=60,
            WaitTimeSeconds=20
        ).get('Messages')
        if is_iterable_value(messages):
            for msg in messages:
                print('Received message: "{}"'.format(msg.get('Body')))
                answer = msg
                # Delete queue
                print('Received message: "{}"'.format(msg.get('Body')))
                received_body = json.loads(msg.get('Body').replace("'", '"'))
                is_answer = received_body.get('is_answer')
                print("type answer: ", type(is_answer))
                if is_answer == 'True':
                    client.delete_queue(QueueUrl=created_queue)
                    print('Successfully deleted temporary queue')
                    break_the_loop = True
                    break
        if break_the_loop:
            break

    return answer


if __name__ == '__main__':
    main()





