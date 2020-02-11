#!/bin/sh
/opt/pyFF/bin/gunicorn \
       	--log-level debug \
	--log-config examples/debug.ini \
	--bind 0.0.0.0:80 \
	-e PYFF_HUB_URL=http://hub.websub.local:8080/hub \
	-e PYFF_HUB_UPDATE=http://hub.websub.local:8080/update \
	-e PYFF_PUBLIC_URL=http://sub.websub.local/ \
	-e PYFF_PIPELINE=sub.fd \
	pyff.wsgi:app
