from kafka.admin import KafkaAdminClient, NewTopic
def setup_kafka():
  def create_topics(admin, topic_list):
    admin.create_topics(new_topics=topic_list, validate_only=False)
    
  admin_client = KafkaAdminClient(bootstrap_servers="34.136.201.42:90922",
                                  client_id='datatengineering-group6')  # use your VM's external IP Here!
  topic_list = [NewTopic(name="chicago_crimes", num_partitions=1, replication_factor=1), NewTopic(name="chicago_crimes_per_community", num_partitions=1, replication_factor=1)]
  create_topics(admin_client, topic_list)

if __name__ == '__main__':
	setup_kafka() # sets up the topics
