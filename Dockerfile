FROM node:18.15.0-slim
MAINTAINER Jonathan Gros-Dubois

LABEL version="9.1.1"
LABEL description="Docker file for SCC State Server"

RUN mkdir -p /usr/src/
WORKDIR /usr/src/
COPY . /usr/src/

RUN npm install .

EXPOSE 7777

CMD ["npm", "start"]
