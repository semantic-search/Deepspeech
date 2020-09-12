

from __future__ import absolute_import, division, print_function

import numpy as np
import shlex
import subprocess
import sys


from deepspeech import Model, version
from timeit import default_timer as timer
try:
    from shhlex import quote
except ImportError:
    from pipes import quote



print('Loading model from file ')
model_load_start = timer()
# ***********************************
ds = Model('deepspeech-0.8.0-models.pbmm')
model_load_end = timer() - model_load_start
print('Loaded model in {:.3}s.'.format(model_load_end), file=sys.stderr)


ds.setBeamWidth(500)
desired_sample_rate = ds.sampleRate()
scorer_load_start = timer()
# ************************************
ds.enableExternalScorer('deepspeech-0.8.0-models.scorer')
scorer_load_end = timer() - scorer_load_start
print('Loaded scorer in {:.3}s.'.format(scorer_load_end), file=sys.stderr)


def convert_samplerate(audio_path, desired_sample_rate):
    sox_cmd = 'sox {} --type raw --bits 16 --channels 1 --rate {} --encoding signed-integer --endian little --compression 0.0 --no-dither - '.format(
        quote(audio_path), desired_sample_rate)
    try:
        output = subprocess.check_output(
            shlex.split(sox_cmd), stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        raise RuntimeError('SoX returned non-zero status: {}'.format(e.stderr))
    except OSError as e:
        raise OSError(e.errno, 'SoX not found, use {}hz files or install it: {}'.format(
            desired_sample_rate, e.strerror))

    return desired_sample_rate, np.frombuffer(output, np.int16)


def metadata_to_string(metadata):
    return ''.join(token.text for token in metadata.tokens)


def predict(metadata):
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


