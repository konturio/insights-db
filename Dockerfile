FROM debian:bullseye-slim

RUN apt-get update && apt-get -y install wget gnupg2 lsb-release
RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

RUN apt-get update && apt-get install -y postgresql-client-16 make parallel

COPY Makefile Makefile
COPY start.sh start.sh

RUN chmod +x start.sh

CMD [ "/start.sh" ]
