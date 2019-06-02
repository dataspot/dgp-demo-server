FROM akariv/dgp-server:latest

ADD requirements.txt /dgp/requirements.txt
RUN python -m pip install -r /dgp/requirements.txt

ADD taxonomies /dgp/taxonomies/
ADD demo_server /dgp/demo_server/

ENV SERVER_MODULE=demo_server.server:app

WORKDIR /dgp/
