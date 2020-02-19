#!/bin/sh
/opt/pyFF/bin/gunicorn \
       	--log-level debug \
	--log-config examples/debug.ini \
	--bind 0.0.0.0:80 \
	-e PYFF_HUB_URL=http://hub.websub.local/hub \
	-e PYFF_HUB_UPDATE=http://hub.websub.local/update \
	-e PYFF_PUBLIC_URL=http://sp.websub.local/ \
	-e PYFF_PIPELINE=sp.fd \
	pyff.wsgi:app
