FROM python:2.7.12

WORKDIR /work

RUN git clone https://github.com/iecasszyjy/tweet_search-master.git
WORKDIR /work/tweet_search-master/spider
RUN pip install -r requirements.txt

CMD ["git", "pull"]#, "&&", "python", "worker.py"]
