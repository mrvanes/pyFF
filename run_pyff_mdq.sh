#!/bin/sh
/opt/pyFF/bin/gunicorn \
    --log-level debug \
    --log-config examples/debug.ini \
    --bind 0.0.0.0:80 \
    --workers 1 \
    --threads 4 \
    --worker-tmp-dir=/dev/shm \
    -e PYFF_HUB_URL=http://hub.websub.local/hub \
    -e PYFF_HUB_UPDATE=http://hub.websub.local/update \
    -e PYFF_PUBLIC_URL=http://mdq.websub.local/ \
    -e PYFF_PIPELINE=mdq.fd \
    -e PYFF_WORKER_POOL_SIZE=4 \
    pyff.wsgi:app
