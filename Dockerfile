FROM tensorflow/tensorflow:2.2.0-gpu
RUN apt-get update
RUN apt-get -y upgrade
RUN apt-get install -y wget
RUN apt-get install -y sox
RUN pip install install fastapi[all]
RUN pip install deepspeech-gpu
RUN mkdir deep_stt
WORKDIR deep_stt
RUN wget https://github.com/mozilla/DeepSpeech/releases/download/v0.8.0/deepspeech-0.8.0-models.pbmm
RUN wget https://github.com/mozilla/DeepSpeech/releases/download/v0.8.0/deepspeech-0.8.0-models.scorer
# EXPOSE 8000
COPY . .
RUN pip install -r requirements.txt
CMD ["python", "main.py"]

# CMD uvicorn main:app --reload --host 0.0.0.0 --port 7000
