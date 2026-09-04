FROM registry.redhat.io/ubi9/ubi-minimal:latest

RUN microdnf install -y python pip git findutils jq
RUN git clone https://github.com/openshift-eng/art-tools && cd art-tools/ocp-build-data-validator && pip install .
RUN pip cache purge \
&& curl -fLo /etc/pki/ca-trust/source/anchors/2022-IT-Root-CA.pem https://certs.corp.redhat.com/certs/2022-IT-Root-CA.pem \
&& update-ca-trust extract

COPY ocp-build-data-validator/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
