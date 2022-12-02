from kafka.admin import KafkaAdminClient, NewTopic
def setup_kafka():
  def create_topics(admin, topic_list):
    admin.create_topics(new_topics=topic_list, validate_only=False)
    
  admin_client = KafkaAdminClient(bootstrap_servers="VM_IP:9092",
                                  client_id='Lab8')  # use your VM's external IP Here!
  topic_list = [NewTopic(name="crimes", num_partitions=1, replication_factor=1)]
  create_topics(admin_client, topic_list)

if __name__ == '__main__':
	setup_kafka() # sets up the topics
