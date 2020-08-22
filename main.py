from __future__ import absolute_import, division, print_function
from fastapi import FastAPI, File, UploadFile, Form, Response, status
import argparse
import numpy as np
import shlex
import subprocess
import sys
import wave
import json
from deepspeech import Model, version
from timeit import default_timer as timer
try:
    from shhlex import quote
except ImportError:
    from pipes import quote
import os
from typing import Optional

print('Loading model from file ')
model_load_start = timer()
#***********************************
ds = Model('deepspeech-0.8.0-models.pbmm')
model_load_end = timer() - model_load_start
print('Loaded model in {:.3}s.'.format(model_load_end), file=sys.stderr)


ds.setBeamWidth(500)
desired_sample_rate = ds.sampleRate()
scorer_load_start = timer()
#************************************
ds.enableExternalScorer('deepspeech-0.8.0-models.scorer')
scorer_load_end = timer() - scorer_load_start
print('Loaded scorer in {:.3}s.'.format(scorer_load_end), file=sys.stderr)




def convert_samplerate(audio_path, desired_sample_rate):
    sox_cmd = 'sox {} --type raw --bits 16 --channels 1 --rate {} --encoding signed-integer --endian little --compression 0.0 --no-dither - '.format(quote(audio_path), desired_sample_rate)
    try:
        output = subprocess.check_output(shlex.split(sox_cmd), stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        raise RuntimeError('SoX returned non-zero status: {}'.format(e.stderr))
    except OSError as e:
        raise OSError(e.errno, 'SoX not found, use {}hz files or install it: {}'.format(desired_sample_rate, e.strerror))

    return desired_sample_rate, np.frombuffer(output, np.int16)


def metadata_to_string(metadata):
    return ''.join(token.text for token in metadata.tokens)


def words_from_candidate_transcript(metadata):
    metadata = metadata.transcripts[0]

    word = ""
    word_list = []
    word_start_time = 0
    # Loop through each character
    for i, token in enumerate(metadata.tokens):
        # Append character to word if it's not a space
        if token.text != " ":
            if len(word) == 0:
                # Log the start time of the new word
                word_start_time = token.start_time

            word = word + token.text
        # Word boundary is either a space or the last character in the array
        if token.text == " " or i == len(metadata.tokens) - 1:
            word_duration = token.start_time - word_start_time

            if word_duration < 0:
                word_duration = 0

            each_word = dict()
            each_word["word"] = word
            each_word["start_time "] = round(word_start_time, 4)
            each_word["duration"] = round(word_duration, 4)

            word_list.append(each_word)
            # Reset
            word = ""
            word_start_time = 0
    print(word_list)
    return word_list

app = FastAPI()
@app.post("/uploadfile/")
def create_upload_file(file: UploadFile = File(...), word_duration: Optional[bool] = Form(False)):
    file_name = file.filename
    with open(file_name, 'wb') as f:
        f.write(file.file.read())
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
    print(word_duration)
    if word_duration:
        response = words_from_candidate_transcript(ds.sttWithMetadata(audio))
    else:
        response = ds.stt(audio)

    os.remove(file_name)
    return response
