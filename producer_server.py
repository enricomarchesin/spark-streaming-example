from kafka import KafkaProducer
import json
import time


class ProducerServer(KafkaProducer):

    def __init__(self, input_file, topic, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.topic = topic

    def generate_data(self):
        with open(self.input_file) as f:
            c = 0
            for e in json.load(f):
                message = self.dict_to_binary(e)
                self.send(self.topic, message)
                print(".", end="")

                c += 1
                if c%100 == 0:
                    print("")
        print()

    def dict_to_binary(self, json_dict):
        return json.dumps(json_dict).encode('utf-8')
