# syntax=docker/dockerfile:1
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

FROM public.ecr.aws/docker/library/python:3.9-buster

ARG AGENT_USER=agentuser
ARG JOB_USER=jobuser
ARG SHARED_GROUP=sharedgroup
ARG CONFIGURE_WORKER_AGENT_CMD
ARG FILE_MAPPINGS

ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_SESSION_TOKEN
ARG AWS_DEFAULT_REGION

RUN <<END_RUN
set -eux

apt-get update && apt-get install sudo && apt-get -y install jq
rm -rf /var/lib/apt/lists/*

curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip.sig" -o "awscliv2.zip.sig"
gpg --import --armor <<EOF
-----BEGIN PGP PUBLIC KEY BLOCK-----

mQINBF2Cr7UBEADJZHcgusOJl7ENSyumXh85z0TRV0xJorM2B/JL0kHOyigQluUG
ZMLhENaG0bYatdrKP+3H91lvK050pXwnO/R7fB/FSTouki4ciIx5OuLlnJZIxSzx
PqGl0mkxImLNbGWoi6Lto0LYxqHN2iQtzlwTVmq9733zd3XfcXrZ3+LblHAgEt5G
TfNxEKJ8soPLyWmwDH6HWCnjZ/aIQRBTIQ05uVeEoYxSh6wOai7ss/KveoSNBbYz
gbdzoqI2Y8cgH2nbfgp3DSasaLZEdCSsIsK1u05CinE7k2qZ7KgKAUIcT/cR/grk
C6VwsnDU0OUCideXcQ8WeHutqvgZH1JgKDbznoIzeQHJD238GEu+eKhRHcz8/jeG
94zkcgJOz3KbZGYMiTh277Fvj9zzvZsbMBCedV1BTg3TqgvdX4bdkhf5cH+7NtWO
lrFj6UwAsGukBTAOxC0l/dnSmZhJ7Z1KmEWilro/gOrjtOxqRQutlIqG22TaqoPG
fYVN+en3Zwbt97kcgZDwqbuykNt64oZWc4XKCa3mprEGC3IbJTBFqglXmZ7l9ywG
EEUJYOlb2XrSuPWml39beWdKM8kzr1OjnlOm6+lpTRCBfo0wa9F8YZRhHPAkwKkX
XDeOGpWRj4ohOx0d2GWkyV5xyN14p2tQOCdOODmz80yUTgRpPVQUtOEhXQARAQAB
tCFBV1MgQ0xJIFRlYW0gPGF3cy1jbGlAYW1hem9uLmNvbT6JAlQEEwEIAD4CGwMF
CwkIBwIGFQoJCAsCBBYCAwECHgECF4AWIQT7Xbd/1cEYuAURraimMQrMRnJHXAUC
ZMKcEgUJCSEf3QAKCRCmMQrMRnJHXCilD/4vior9J5tB+icri5WbDudS3ak/ve4q
XS6ZLm5S8l+CBxy5aLQUlyFhuaaEHDC11fG78OduxatzeHENASYVo3mmKNwrCBza
NJaeaWKLGQT0MKwBSP5aa3dva8P/4oUP9GsQn0uWoXwNDWfrMbNI8gn+jC/3MigW
vD3fu6zCOWWLITNv2SJoQlwILmb/uGfha68o4iTBOvcftVRuao6DyqF+CrHX/0j0
klEDQFMY9M4tsYT7X8NWfI8Vmc89nzpvL9fwda44WwpKIw1FBZP8S0sgDx2xDsxv
L8kM2GtOiH0cHqFO+V7xtTKZyloliDbJKhu80Kc+YC/TmozD8oeGU2rEFXfLegwS
zT9N+jB38+dqaP9pRDsi45iGqyA8yavVBabpL0IQ9jU6eIV+kmcjIjcun/Uo8SjJ
0xQAsm41rxPaKV6vJUn10wVNuhSkKk8mzNOlSZwu7Hua6rdcCaGeB8uJ44AP3QzW
BNnrjtoN6AlN0D2wFmfE/YL/rHPxU1XwPntubYB/t3rXFL7ENQOOQH0KVXgRCley
sHMglg46c+nQLRzVTshjDjmtzvh9rcV9RKRoPetEggzCoD89veDA9jPR2Kw6RYkS
XzYm2fEv16/HRNYt7hJzneFqRIjHW5qAgSs/bcaRWpAU/QQzzJPVKCQNr4y0weyg
B8HCtGjfod0p1A==
=gdMc
-----END PGP PUBLIC KEY BLOCK-----
EOF
gpg --verify awscliv2.zip.sig awscliv2.zip
unzip -qq awscliv2.zip
sudo ./aws/install

groupadd --system $SHARED_GROUP
useradd --create-home --system --shell=/bin/bash --groups=$SHARED_GROUP $JOB_USER
useradd --create-home --system --shell=/bin/bash --groups=$SHARED_GROUP $AGENT_USER
echo "$AGENT_USER ALL=($AGENT_USER,$JOB_USER) NOPASSWD: ALL" > /etc/sudoers.d/$AGENT_USER
END_RUN

COPY --chown=$AGENT_USER:$AGENT_USER file_mappings /file_mappings

COPY --chown=root:root <<-EOF /entrypoint.sh
#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

set -euxo pipefail

# Copy over file mappings
file_mappings='$FILE_MAPPINGS' 
file_mappings=\$(echo "\$file_mappings" | jq -cr 'to_entries[] | "\\(.key):\\(.value)"')
for mapping in \$file_mappings; do
    IFS=: read -r src dst <<< "\$mapping"
    mkdir -p "\$(dirname \$dst)"
    mv "\$src" "\$dst"
done

# Configure the Worker agent
$CONFIGURE_WORKER_AGENT_CMD

sudo --preserve-env -H -u $AGENT_USER "\$@"
EOF

COPY --chown=$AGENT_USER:$AGENT_USER --chmod=750 <<-EOF /home/$AGENT_USER/run_agent.sh
#!/bin/bash
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

set -euxo pipefail

deadline-worker-agent --allow-instance-profile --no-shutdown
EOF

ENV AGENT_USER $AGENT_USER
USER root
WORKDIR /
ENTRYPOINT ["/bin/bash", "/entrypoint.sh"]
CMD ["/bin/bash", "-c", "/home/$AGENT_USER/run_agent.sh"]
