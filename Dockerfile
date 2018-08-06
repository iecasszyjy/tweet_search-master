FROM python:2.7.12

WORKDIR /work

RUN git clone https://github.com/iecasszyjy/tweet_search-master.git
RUN apt-get update 
RUN apt-get install libxml2-dev libxslt-dev python-dev zlib1g-dev
WORKDIR /work/tweet_search/spider
RUN pip install -r requirements.txt

CMD ["git", "pull"]#, "&&", "python", "worker.py"]
