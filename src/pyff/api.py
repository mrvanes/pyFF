from pyramid.config import Configurator
from pyramid.response import Response
import pyramid.httpexceptions as exc
from .exceptions import ResourceException
from .constants import config
import importlib
from .pipes import plumbing
from .samlmd import entity_display_name
from six.moves.urllib_parse import quote_plus
from six import b
from .logs import get_log
from json import dumps
from datetime import datetime, timedelta
from .utils import dumptree, duration2timedelta, hash_id, json_serializer, b2u
from .repo import MDRepository
import pkg_resources
from accept_types import AcceptableType
from lxml import etree
from pyramid.events import NewRequest
import requests
import threading
import pytz
from .utils import url_get, url_post, hash_dict
from .subscriber import subscriber, parse_lease_seconds
from .resource import Resource

log = get_log(__name__)


def robots_handler(request):
    return Response("""
User-agent: *
Disallow: /
""")


def status_handler(request):
    d = {}
    for r in request.registry.md.rm:
        if 'Validation Errors' in r.info and r.info['Validation Errors']:
            d[r.url] = r.info['Validation Errors']
    _status = dict(version=pkg_resources.require("pyFF")[0].version,
                   invalids=d,
                   icon_store=dict(size=request.registry.md.icon_store.size()),
                   jobs=[dict(id=j.id, next_run_time=j.next_run_time)
                         for j in request.registry.scheduler.get_jobs()],
                   threads=[t.name for t in threading.enumerate()],
                   store=dict(size=request.registry.md.store.size()))
    response = Response(dumps(_status, default=json_serializer))
    response.headers['Content-Type'] = 'application/json'
    return response


class MediaAccept(object):

    def __init__(self, accept):
        self._type = AcceptableType(accept)

    def has_key(self, key):
        return True

    def get(self, item):
        return self._type.matches(item)

    def __contains__(self, item):
        return self._type.matches(item)

    def __str__(self):
        return str(self._type)


def _fmt(data, accepter):
    if data is None or len(data) == 0:
        return "", 'text/plain'
    if isinstance(data, (etree._Element, etree._ElementTree)) and (
            accepter.get('text/xml') or accepter.get('application/xml') or accepter.get(
        'application/samlmetadata+xml')):
        return dumptree(data), 'application/samlmetadata+xml'
    #if isinstance(data, (etree._Element, etree._ElementTree)):
        #return dumptree(data), 'application/xml'
    if isinstance(data, (dict, list)) and accepter.get('application/json'):
        return dumps(data, default=json_serializer), 'application/json'

    raise exc.exception_response(406)


def call(entry):
    requests.post('{}/api/call/{}'.format(config.base_url, entry))


def process_handler(request):
    _ctypes = {'xml': 'application/xml',
               'json': 'application/json'}

    def _d(x, do_split=True):
        if x is not None:
            x = x.strip()

        if x is None or len(x) == 0:
            return None, None

        if '.' in x:
            (pth, dot, extn) = x.rpartition('.')
            assert (dot == '.')
            if extn in _ctypes:
                return pth, extn

        return x, None

    #log.debug(request)

    if request.matchdict is None:
        raise exc.exception_response(400)

    if request.body:
        try:
            request.matchdict.update(request.json_body)
        except ValueError as ex:
            pass

    entry = request.matchdict.get('entry', 'request')
    path = list(request.matchdict.get('path', []))
    match = request.params.get('q', request.params.get('query', None))

    # Enable matching on scope.
    match = (match.split('@').pop() if match and not match.endswith('@')
             else match)
    log.debug("match={}".format(match))

    if 0 == len(path):
        path = ['entities']

    alias = path.pop(0)
    path = '/'.join(path)

    # Ugly workaround bc WSGI drops double-slashes.
    path = path.replace(':/', '://')

    msg = "handling entry={}, alias={}, path={}"
    log.debug(msg.format(entry, alias, path))

    pfx = None
    if 'entities' not in alias:
        pfx = request.registry.aliases.get(alias, None)
        if pfx is None:
            raise exc.exception_response(404)

    path, ext = _d(path, True)
    if pfx and path:
        q = "{%s}%s" % (pfx, path)
        path = "/%s/%s" % (alias, path)
    else:
        q = path

    # TODO - sometimes the client sends > 1 accept header value with ','.
    accept = str(request.accept).split(',')[0]
    # import pdb; pdb.set_trace()
    log.debug("accept: {}".format(accept))
    if (not accept or 'application/*' in accept or 'text/*' in accept or '*/*' in accept) and ext:
        accept = _ctypes[ext]

    try:
        accepter = MediaAccept(accept)
        for p in request.registry.plumbings:
            state = {entry: True,
                     'headers': {'Content-Type': None},
                     'accept': accepter,
                     'url': request.current_route_url(),
                     'select': q,
                     'match': match.lower() if match else match,
                     'path': path,
                     'stats': {}}

            r = p.process(request.registry.md,
                          state=state,
                          raise_exceptions=True,
                          scheduler=request.registry.scheduler)
            #log.debug(r)
            if r is None:
                r = []

            response = Response()
            response.headers.update(state.get('headers', {}))
            ctype = state.get('headers').get('Content-Type', None)
            if not ctype:
                r, t = _fmt(r, accepter)
                ctype = t

            response.text = b2u(r)
            response.size = len(r)
            response.content_type = ctype
            cache_ttl = int(state.get('cache', 0))
            response.expires = (datetime.now() + timedelta(seconds=cache_ttl))
            return response
    except ResourceException as ex:
        import traceback
        log.debug(traceback.format_exc())
        log.warn(ex)
        raise exc.exception_response(409)
    except BaseException as ex:
        import traceback
        log.debug(traceback.format_exc())
        log.error(ex)
        raise exc.exception_response(500)

    if request.method == 'GET':
        raise exc.exception_response(404)

def update_handler(request):
    entry = request.POST.get('entry', None)
    log.debug("update_handler {}".format(entry))

    log.debug("registry: {}".format(request.registry))
    log.debug("md: {}".format(request.registry.md))
    log.debug("store: {}".format(request.registry.md.store))
    log.debug("rm: {}".format(request.registry.md.rm))
    log.debug("rm.url: {}".format(request.registry.md.rm.url))

    for r in request.registry.md.rm:
        log.debug("r: {}".format(r))
        log.debug("info: {}".format(r.info))
        log.debug("url: {}".format(r.url))
        entities = r.info.get('Entities', [])
        old_hash = r.info.get('Enthash', None)

        # We should only send updates for entities
        # That actually changed
        if entities and entry in entities:
            log.debug("entry matched: {}".format(entry))

            # Refresh source MD for this URL Resource
            request.registry.md.rm.reload(url=r.url)
            # Call Hub update callback for entityID config.hub_url
            params = { 'topic': config.public_url.strip("/") + "/entities/" + entry }
            # Call Hub update callback for {sha1}entityID config.hub_url
            url_post(config.hub_update, params)
            params = { 'topic': config.public_url.strip("/") + "/entities/%s" % hash_id(entry) }
            url_post(config.hub_update, params)

            # Send updates for the webfinger endpoint?
            new_hash = hash_dict(r.e_hash)
            if new_hash != old_hash:
                log.debug("Updating webfinger endpoint reload {}/{}".format(new_hash, old_hash))
                params = { 'topic': config.public_url.strip("/") + "/.well-known/webfinger" }
                url_post(config.hub_update, params)
            else:
                log.debug("Skipping webfinger endpoint reload {}/{}".format(new_hash, old_hash))

    response = Response("OK\n")
    return response

# WebSub API implementation
def callback_handler(request):
    callback_id = request.matchdict.get('callback_id', None)
    log.debug("callback_handler {}".format(callback_id))

    # If this is POST this is an update notify
    if request.method == 'POST':
        subscription = subscriber.storage[callback_id]
        if subscription == None:
            log.debug("callback_id not found {}".format(callback_id))
            raise exc.exception_response(410)

        response = Response('Content Received!\n')
        topic_url = subscription.get('topic_url', None)

        t = threading.Thread(target=handle_reload, args=(request, topic_url))
        t.start()

        # End of the POST
        return response

    # It's GET
    mode = request.GET.get('hub.mode', None)
    topic_url = request.GET.get('hub.topic', None)
    lease_seconds = request.GET.get('hub.lease_seconds', None)
    challenge = request.GET.get('hub.challenge', None)
    response = Response(challenge)

    if mode == 'denied':
        log.debug("topic denied")
        subscriber.temp_storage.pop(callback_id)
        return response
    elif mode in ['subscribe', 'unsubscribe']:
        subscription_request = subscriber.temp_storage.pop(callback_id)
        if not subscription_request:
            return response

        if mode != subscription_request['mode']:
            raise exc.exception_response(404)
        if topic_url != subscription_request['topic_url']:
            raise exc.exception_response(404)
        if mode == 'subscribe':
            lease = parse_lease_seconds(lease_seconds)
            if lease:
                subscription_request['lease_seconds'] = lease
            else:
                raise exc.exception_response(404)
            subscriber.storage[callback_id] = subscription_request
        else:  # unsubscribe
            del subscriber.storage[callback_id]

        return response
    else:
        raise exc.exception_response(400)


def handle_reload(request, topic_url):
    log.debug("Updating resource: {}".format(topic_url))
    resource = request.registry.md.rm.find(topic_url)

    # IMPORTANT First reload, then notify otherwise
    # Downstream will see stale resource!!
    # We should execute this in background!
    try:
        # TODO This is weird!
        # This works:
        request.registry.md.rm.reload(url=topic_url)
        # But this doesn't?
        #resource.reload()
    except Exception as e:
        log.debug("Reload failed: {}".format(e))

    if isinstance(resource, Resource):
        entities = resource.info.get('Entities', [])
        old_hash = resource.info.get('Enthash', None)
    else:
        entities = []

    if entities:
        # Update webfinger endpoint?
        new_hash = hash_dict(resource.e_hash)
        if new_hash != old_hash:
            log.debug("Updating webfinger endpoint reload {}/{}".format(new_hash, old_hash))
            params = { 'topic': config.public_url.strip("/") + "/.well-known/webfinger" }
            url_post(config.hub_update, params)
        else:
            log.debug("Skipping webfinger endpoint reload {}/{}".format(new_hash, old_hash))

    for entity in entities:
        if resource.e_notify.get(entity, False):
            log.debug("Notifying entity: {}".format(entity))
            #params = { 'topic': config.public_url.strip("/") + "/entities/" + entity }
            #r = url_post(config.hub_update, params)
            params = { 'topic': config.public_url.strip("/") + "/entities/%s" % hash_id(entity) }
            r = url_post(config.hub_update, params)
            del resource.e_notify[entity]
        else:
            log.debug("Skipping entity: {}".format(entity))

    #log.debug("Resource tree")
    #request.registry.md.rm.tree()


def webfinger_handler(request):
    """An implementation the webfinger protocol
(http://tools.ietf.org/html/draft-ietf-appsawg-webfinger-12)
in order to provide information about up and downstream metadata available at
this pyFF instance.

Example:

.. code-block:: bash

# curl http://my.org/.well-known/webfinger?resource=http://my.org

This should result in a JSON structure that looks something like this:

.. code-block:: json

{
 "expires": "2013-04-13T17:40:42.188549",
 "links": [
 {
  "href": "http://reep.refeds.org:8080/role/sp.xml",
  "rel": "urn:oasis:names:tc:SAML:2.0:metadata"
  },
 {
  "href": "http://reep.refeds.org:8080/role/sp.json",
  "rel": "disco-json"
  }
 ],
 "subject": "http://reep.refeds.org:8080"
}

Depending on which version of pyFF your're running and the configuration you
may also see downstream metadata listed using the 'role' attribute to the link
elements.
        """

    resource = request.params.get('resource', None)
    rel = request.params.get('rel', None)

    if resource is None:
        resource = request.host_url

    jrd = dict()
    dt = datetime.now() + duration2timedelta("PT1H")
    jrd['expires'] = dt.isoformat()
    jrd['subject'] = request.host_url
    links = list()
    jrd['links'] = links

    _dflt_rels = {
        'urn:oasis:names:tc:SAML:2.0:metadata': ['.xml', 'application/xml'],
        'disco-json': ['.json', 'application/json']
    }

    if rel is None or len(rel) == 0:
        rel = _dflt_rels.keys()
    else:
        rel = [rel]

    def _links(url, title=None, fp=''):
        if url.startswith('/'):
            url = url.lstrip('/')
        for r in rel:
            suffix = ""
            if not url.endswith('/'):
                suffix = _dflt_rels[r][0]
            links.append(dict(rel=r,
                              type=_dflt_rels[r][1],
                              href='%s/%s%s' % (config.public_url.strip('/'), url, suffix),
                              fp=fp
                              )
                         )

    _links('/entities/')
    for a in request.registry.md.store.collections():
        if a is not None and '://' not in a:
            _links(a)

    for entity in request.registry.md.store.lookup('entities'):
        e_hash = hash_id(''.join(s.strip() for s in entity.itertext()))
        entity_display = entity_display_name(entity)
        #log.debug("entity: {}, {}".format(entity_display, e_hash))
        _links("/entities/%s" % hash_id(entity.get('entityID')),
               title=entity_display, fp=e_hash)

    aliases = request.registry.aliases
    for a in aliases.keys():
        for v in request.registry.md.store.attribute(aliases[a]):
            _links('%s/%s' % (a, quote_plus(v)))

    response = Response(dumps(jrd, default=json_serializer))
    response.headers['Content-Type'] = 'application/json'

    #Create a publisher header dict, one for self and one for hub
    #Link: <http://pub.websub.local/md>; rel="self", <http://pub.websub.local/hub>; rel="hub"
    hub_url = config.hub_url
    if hub_url:
      log.debug("hub_url: {}".format(hub_url))
      #path = "/.well-known/webfinger"
      path = request.path_qs
      pub = {
        'self': config.public_url.strip('/') + path,
        'hub': hub_url
      }
      h = ', '.join([ "<"+v+">; rel=\""+k+"\"" for k,v in pub.items() ])
      response.headers['Link'] = h

    return response


def resources_handler(request):
    def _info(r):
        nfo = r.info
        nfo['Valid'] = r.is_valid()
        nfo['Parser'] = r.last_parser
        if r.last_seen is not None:
            nfo['Last Seen'] = r.last_seen
        if len(r.children) > 0:
            nfo['Children'] = [_info(cr) for cr in r.children]
        return nfo

    _resources = [_info(r) for r in request.registry.md.rm.children]
    response = Response(dumps(_resources, default=json_serializer))
    response.headers['Content-Type'] = 'application/json'

    return response


def pipeline_handler(request):
    response = Response(dumps(request.registry.plumbings,
                              default=json_serializer))
    response.headers['Content-Type'] = 'application/json'

    return response


def search_handler(request):
    match = request.params.get('q', request.params.get('query', None))

    # Enable matching on scope.
    match = (match.split('@').pop() if match and not match.endswith('@')
             else match)

    entity_filter = request.params.get('entity_filter',
                                       '{http://pyff.io/role}idp')
    log.debug("match={}".format(match))
    store = request.registry.md.store

    def _response():
        yield b('[')
        in_loop = False
        entities = store.search(query=match.lower(),
                                entity_filter=entity_filter)
        for e in entities:
            if in_loop:
                yield b(',')
            yield b(dumps(e))
            in_loop = True
        yield b(']')

    response = Response(content_type='application/json')
    response.app_iter = _response()
    return response


def add_cors_headers_response_callback(event):
    def cors_headers(request, response):
        response.headers.update({
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST,GET,DELETE,PUT,OPTIONS',
            'Access-Control-Allow-Headers': ('Origin, Content-Type, Accept, '
                                             'Authorization'),
            'Access-Control-Allow-Credentials': 'true',
            'Access-Control-Max-Age': '1728000',
        })

    event.request.add_response_callback(cors_headers)


def launch_memory_usage_server(port=9002):
    import cherrypy
    import dowser

    cherrypy.tree.mount(dowser.Root())
    cherrypy.config.update({
        'environment': 'embedded',
        'server.socket_port': port
    })

    cherrypy.engine.start()


def mkapp(*args, **kwargs):
    md = kwargs.pop('md', None)
    if md is None:
        md = MDRepository()

    if config.devel_memory_profile:
        launch_memory_usage_server()

    with Configurator(debug_logger=log) as ctx:
        ctx.add_subscriber(add_cors_headers_response_callback, NewRequest)

        if config.aliases is None:
            config.aliases = dict()

        if config.modules is None:
            config.modules = []

        ctx.registry.config = config
        config.modules.append('pyff.builtins')
        for mn in config.modules:
            importlib.import_module(mn)

        pipeline = args or None
        if pipeline is None and config.pipeline:
            pipeline = [config.pipeline]

        ctx.registry.scheduler = md.scheduler
        if pipeline is not None:
            ctx.registry.pipeline = pipeline
            ctx.registry.plumbings = [plumbing(v) for v in pipeline]
        ctx.registry.aliases = config.aliases
        ctx.registry.md = md

        ctx.add_route('robots', '/robots.txt')
        ctx.add_view(robots_handler, route_name='robots')

        ctx.add_route('webfinger', '/.well-known/webfinger',
                      request_method='GET')
        ctx.add_view(webfinger_handler, route_name='webfinger')

        ctx.add_route('search', '/api/search', request_method='GET')
        ctx.add_view(search_handler, route_name='search')

        ctx.add_route('status', '/api/status', request_method='GET')
        ctx.add_view(status_handler, route_name='status')

        ctx.add_route('resources', '/api/resources', request_method='GET')
        ctx.add_view(resources_handler, route_name='resources')

        ctx.add_route('pipeline', '/api/pipeline', request_method='GET')
        ctx.add_view(pipeline_handler, route_name='pipeline')

        ctx.add_route('call', '/api/call/{entry}',
                      request_method=['POST', 'PUT'])
        ctx.add_view(process_handler, route_name='call')

        ctx.add_route('update', '/api/update',
                      request_method=['POST'])
        ctx.add_view(update_handler, route_name='update')

        callback = config.subscriber_callback_endpoint
        ctx.add_route('callback', callback + '/{callback_id}',
                      request_method=['GET', 'POST'])
        ctx.add_view(callback_handler, route_name='callback')

        ctx.add_route('request', '/*path', request_method='GET')
        ctx.add_view(process_handler, route_name='request')

        start = datetime.now() + timedelta(seconds=1)
        log.debug(start)
        if config.update_frequency > 0: #schedule interval update
            ctx.registry.scheduler.add_job(call,
                                           'interval',
                                           id="call/update",
                                           args=['update'],
                                           start_date=start,
                                           seconds=config.update_frequency,
                                           replace_existing=True,
                                           max_instances=1,
                                           timezone=pytz.utc)
        else: #run update now to populate MDQ
            ctx.registry.scheduler.add_job(call,
                                          'date',
                                          id='initialise',
                                          args=['update'],
                                          next_run_time=start,
                                          misfire_grace_time=60)
        return ctx.make_wsgi_app()
