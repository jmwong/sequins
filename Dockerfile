# Ubuntu 12.04
FROM dockerregistry.service.consul/stripe/ubuntu-12.04

RUN apt-get update && apt-get install -y tree cowsay coreutils wget curl git mercurial build-essential

# Go 1.4.2
RUN curl --silent --location https://golang.org/dl/go1.4.2.linux-amd64.tar.gz > /tmp/go.tar.gz
ADD go.tar.gz.sha512 /tmp/go.tar.gz.sha512
RUN shasum -p -a 512 -c /tmp/go.tar.gz.sha512
RUN tar --directory=/usr/local/ -xzf /tmp/go.tar.gz
ENV PATH $PATH:/usr/local/go/bin

# Test & Build
RUN mkdir -p /sequins
RUN mkdir -p /build
ADD . /sequins
WORKDIR /sequins
CMD /sequins/test_and_build.sh
