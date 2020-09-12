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





def send_to_topic(topic, value_to_send_dic):
    data_json = json.dumps(value_to_send_dic)
    init.producer_obj.send(topic, value=data_json)

if __name__=="__main__":
    for message in init.consumer_obj:
        global_init()
        message = message.value
        db_key = str(message)
        db_object = Cache.objects.get(pk=db_key)
        file_name = db_object.file_name
        init.redis_obj.set(globals.RECEIVE_TOPIC, file_name)
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

        os.remove(file_name)

        print(response)

        # sending full and text res(without cordinates or probability) to kafka
        send_to_topic(globals.SEND_TOPIC_TEXT, value_to_send_dic=response)
        send_to_topic(globals.SEND_TOPIC_FULL, value_to_send_dic=response)
        init.producer_obj.flush()