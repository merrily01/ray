# syntax=docker/dockerfile:1.3-labs

FROM cr.ray.io/rayproject/forge

RUN <<EOF
#!/bin/bash

set -euo pipefail

mkdir -p "$HOME/fossa"

curl -sSfL https://github.com/fossas/fossa-cli/releases/download/v3.10.6/fossa_3.10.6_linux_amd64.tar.gz \
    | tar -xvzf - -C "$HOME/fossa"

pip install pandas
wget -O /tmp/askalono.zip https://github.com/jpeddicord/askalono/releases/download/0.5.0/askalono-Linux.zip 
unzip -d /tmp/ /tmp/askalono.zip
mv /tmp/askalono $HOME/.local/bin/

EOF

CMD ["echo", "ray fossa"]
