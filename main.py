from __future__ import absolute_import, division, print_function

import wave

try:
    from shhlex import quote
except ImportError:
    from pipes import quote
import os

import json
from db_models.mongo_setup import global_init
from db_models.models.cache_model import Cache
import init
from deep_speech import *
import globals
import pyfiglet
import requests
from init import ERR_LOGGER

global_init()

def update_state(file_name):
    payload = {
        'parent_name': globals.PARENT_NAME,
        'group_name': globals.GROUP_NAME,
        'container_name': globals.RECEIVE_TOPIC,
        'file_name': file_name,
        'client_id': globals.CLIENT_ID
    }
    try:
        requests.request("POST", globals.DASHBOARD_URL,  data=payload)
    except:
        print(f"{e} ERROR IN SAVE TO DB FILE ID {FILE_ID}")
        ERR_LOGGER(f"{e} ERROR IN SAVE TO DB FILE ID {FILE_ID}")


if __name__=="__main__":
    print(pyfiglet.figlet_format(str(globals.RECEIVE_TOPIC)))
    print(pyfiglet.figlet_format("INDEXING CONTAINER"))
    print("Connected to Kafka at " + globals.KAFKA_HOSTNAME + ":" + globals.KAFKA_PORT)
    print("Kafka Consumer topic for this Container is " + globals.RECEIVE_TOPIC)
    for message in init.consumer_obj:

        message = message.value
        db_key = str(message)
        try:
            db_object = Cache.objects.get(pk=db_key)
        except:
            print(f"{e} EXCEPTION IN GET PK... continue")
            ERR_LOGGER(f"{e} EXCEPTION IN GET PK... continue")
            continue

        file_name = db_object.file_name
        print("#############################################")
        print("########## PROCESSING FILE " + file_name)
        print("#############################################")
        # data = message['data']
        # word_duration = message['word_duration'] # OPTIONAL

        # Setting image-id to topic name(container name), so we can know which image it's currently processing


        with open(file_name, "wb") as fh:
            fh.write(db_object.file.read())
        fin = wave.open(file_name, 'rb')
        fs_orig = fin.getframerate()
        if fs_orig != desired_sample_rate:
            print(
                'Warning: original sample rate ({}) is different than {}hz. Resampling might produce erratic speech recognition.'.format(
                    fs_orig, desired_sample_rate), file=sys.stderr)
            fs_new, audio = convert_samplerate(file_name, desired_sample_rate)
        else:
            audio = np.frombuffer(fin.readframes(fin.getnframes()), np.int16)
        fin.close()


        response = ds.stt(audio)
        print(response)
        if response :
            db_object.is_stt=True
            toAdd = response[0]
            db_object.text = toAdd
            db_object.save()
        else:
            print('No data to show')

        os.remove(file_name)
        print(".....................FINISHED PROCESSING FILE.....................")
        try:

            update_state(file_name)
        except Exception as e:
            print(f"{e} ERROR IN PREDICT")
            ERR_LOGGER(f"{e} Exception in predict FILE ID {FILE_ID}")

        # print(response)
