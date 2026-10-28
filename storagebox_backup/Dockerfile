ARG BUILD_FROM
FROM ${BUILD_FROM}

RUN apk add --no-cache borgbackup openssh-client sshpass python3 py3-flask tzdata

COPY app /app
COPY run.sh /run.sh
RUN chmod a+x /run.sh

CMD ["/run.sh"]
