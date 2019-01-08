FROM node:10-slim
MAINTAINER Jonathan Gros-Dubois

LABEL version="1.0.1"
LABEL description="Docker file for AGC State Server"

RUN mkdir -p /usr/src/
WORKDIR /usr/src/
COPY . /usr/src/

RUN npm install .

EXPOSE 7777

CMD ["npm", "start"]
