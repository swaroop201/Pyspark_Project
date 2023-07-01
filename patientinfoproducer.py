#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# A simple example demonstrating use of JSONSerializer.
import pickle
import argparse
from uuid import uuid4
from six.moves import input
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.schema_registry import *
import pandas as pd
from confluent_kafka.avro.cached_schema_registry_client import CachedSchemaRegistryClient
from typing import List
import json

FILE_PATH = "C:\\Users\\user\\Desktop\\covid\\PatientInfo.csv"
columns=['patient_id','sex','age','country','province',
	'city','infection_case','infected_by','contact_number','symptom_onset_date','confirmed_date','released_date',
	'deceased_date','state'
]

API_KEY = 'WMCUMEYFGLQTJX3N'
ENDPOINT_SCHEMA_URL  = 'https://psrc-8kz20.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'NkFTWZVl1Wm04QXlVoA8mo9PuXqSyQeyuN9X72lUOpmiHzjyZXwtsN1s4xaEI/zy'
BOOTSTRAP_SERVER = 'pkc-41p56.asia-south1.gcp.confluent.cloud:9092'
SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'FGIIYDQ3SF472BPT'
SCHEMA_REGISTRY_API_SECRET = 'BEI9FKB6ozKg58x1TcsP6TdF8bVw1XIsMCL3WNJwWIkpIMWWizn7L78xEZWgFYEZ'


def sasl_conf():

    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                #  'security.protocol': 'SASL_PLAINTEXT'}
                'bootstrap.servers':BOOTSTRAP_SERVER,
                'security.protocol': SECURITY_PROTOCOL,
                'sasl.username': API_KEY,
                'sasl.password': API_SECRET_KEY
                }
    return sasl_conf



def schema_config():
    return {'url':ENDPOINT_SCHEMA_URL,
    
    'basic.auth.user.info':f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

    }


class Patient:   
    def __init__(self,record:dict):
        for k,v in record.items():
            setattr(self,k,v)
        
        self.record=record
   
    @staticmethod
    def dict_to_patient(data:dict,ctx):
        return Patient(record=data)

    def __str__(self):
        return f"{self.record}"


def get_patient_instance(file_path):
    df=pd.read_csv(file_path)
    df=df.iloc[:,0:]
    df=df.fillna("NAN")
    df = df.astype(str) 
    patients:List[Patient]=[]
    for data in df.values:
        patient=Patient(dict(zip(columns,data)))
        patients.append(patient)
        yield patient

def patient_to_dict(patient:Patient, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    return patient.record


def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):
   
    schema_str = """
    {
  "$id": "http://example.com/myURI.schema.json",
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "description": "Sample schema to help you get started.",
  "properties": {
    "patient_id": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "sex": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "age": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "country": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "province": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "city": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "infection_case": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "infected_by": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
    "contact_number": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
	"symptom_onset_date": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
	"confirmed_date": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
	"released_date": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
	"deceased_date": {
      "description": "The type(v) type is used.",
      "type": "string"
    },
	"state": {
      "description": "The type(v) type is used.",
      "type": "string"
    }
	
  },
  "title": "SampleRecord",
  "type": "object"
}
    """
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    string_serializer = StringSerializer('utf_8')
    json_serializer = JSONSerializer(schema_str, schema_registry_client, patient_to_dict)
    
    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    #while True:
        # Serve on_delivery callbacks from previous calls to produce() 
    producer.poll(0.0)
    try:
        for patient in get_patient_instance(file_path=FILE_PATH):

            print(patient)
            producer.produce(topic=topic,
                            key=string_serializer(str(uuid4()), patient_to_dict),
                            value=json_serializer(patient, SerializationContext(topic, MessageField.VALUE)),
                            on_delivery=delivery_report)
           
    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()
  
    
    

main("patient-info")
