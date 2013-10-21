# Copyright 2013 University of Chicago

import sys
import base64
import httplib  # for status codes
import json
import xml.etree.ElementTree as ET

import httplib2  # Not in standard library, change later if needed.


##############################################################################
# Nimbus context broker client
##############################################################################


Connection = httplib2.Http


class ContextClient(object):
    """Broker connection management and utility functionality.
    """
    def __init__(self, broker_uri, key, secret):
        self.broker_uri = broker_uri
        self.connection = Connection(disable_ssl_certificate_validation=True)
        self.connection.add_credentials(key, secret)

        # forcibly add basic auth header to get around Nimbus not
        # sending WWW-Authenticate header in 401 response, which
        # httplib2 relies on.
        auth = base64.b64encode(key + ":" + secret)
        self.headers = {'Authorization': 'Basic ' + auth}

    def create_context(self):
        """Create a new context with Broker.

        @return: instance of type C{ContextResource}
        """

        # creating a new context is a POST to the base broker URI
        # context URI is returned in Location header and info for VMs
        # is returned in body
        try:
            (resp, body) = self.connection.request(self.broker_uri, 'POST',
                    headers=self.headers)
        except Exception, e:
            trace = sys.exc_info()[2]
            raise BrokerError("Failed to contact broker: %s" % str(e)), None, trace

        if resp.status != httplib.CREATED:
            raise BrokerError("Failed to create new context")

        location = resp['location']
        body = json.loads(body)

        return _resource_from_response(location, body)

    def get_status(self, resource):
        """Status of a Context resource.

        Returns a ContextStatus object
        """

        try:
            (resp, body) = self.connection.request(str(resource), 'GET',
                    headers=self.headers)
        except Exception, e:
            trace = sys.exc_info()[2]
            raise BrokerError("Failed to contact broker: %s" % str(e)), None, trace

        if resp.status != httplib.OK:

            if resp.status == httplib.NOT_FOUND:
                raise ContextNotFoundError("Context resource not found: %s" % resource)
            elif resp.status in (httplib.FORBIDDEN, httplib.UNAUTHORIZED):
                raise BrokerAuthError("User not authorized for context: %s" % resource)

            raise BrokerError("Failed to get status of context")
        try:
            response = json.loads(body)
            return _status_from_response(response)
        except:
            raise BrokerError("Failed to parse status response from broker")


class ContextResource(dict):
    """Context created on the broker.

    Used in generation of userdata.
    """
    def __init__(self, **kwargs):
        for key, value in kwargs.iteritems():
            self[key] = value
        self.uri = self['uri']
        self.broker_uri = self['broker_uri']
        self.context_id = self['context_id']
        self.secret = self['secret']

    def __str__(self):
        return self.uri


def _resource_from_response(uri, response):
    dct = {'broker_uri': response['brokerUri'],
            'context_id': response['contextId'],
            'secret': response['secret'],
            'uri': uri}
    return ContextResource(**dct)


def _status_from_response(response):
    res_nodes = response['nodes']
    nodes = []
    for n in res_nodes:
        ids = _identities_from_response_node(n)
        ok_occurred = n.get('okOccurred', False)
        error_occurred = n.get('errorOccurred', False)
        error_code = n.get('errorCode')
        error_message = n.get('errorMessage', None)
        node = ContextNode(ids, ok_occurred, error_occurred, error_code,
                error_message)
        nodes.append(node)

    complete = response.get('isComplete', False)
    error = response.get('errorOccurred', False)
    expected_count = response['expectedNodeCount']
    return ContextStatus(nodes, expected_count, complete, error)


def _identities_from_response_node(resp_node):
    ids = resp_node['identities']
    identities = []
    for id in ids:
        identity = ContextNodeIdentity(id['iface'], id['ip'], id['hostname'],
            id['pubkey'])
        identities.append(identity)
    return identities


class ContextStatus(object):
    """Status information about a context
    """
    def __init__(self, nodes, expected_count, complete=False, error=False):
        self.nodes = nodes
        self.expected_count = expected_count
        self.complete = complete
        self.error = error


class ContextNode(object):
    """A single contextualization node, with one or more identities.
    """
    def __init__(self, identities, ok_occurred=False, error_occurred=False,
            error_code=None, error_message=None):
        self.identities = identities
        self.ok_occurred = ok_occurred
        self.error_occurred = error_occurred
        self.error_code = error_code
        self.error_message = error_message


class ContextNodeIdentity(object):
    """A single network identity for a node.
    """
    def __init__(self, interface, ip, hostname, pubkey):
        self.interface = interface
        self.ip = ip
        self.hostname = hostname
        self.pubkey = pubkey


class BrokerError(Exception):
    """Error response from Context Broker.
    """
    def __init(self, reason):
        self.reason = reason
        Exception.__init__(self, reason)


class ContextNotFoundError(BrokerError):
    """404 Error response from Context Broker
    """


class BrokerAuthError(BrokerError):
    """403 Error response from Context Broker
    """

##############################################################################
# Nimbus cluster document parsing
##############################################################################


NS_CTXBROKER = "http://www.globus.org/2008/12/nimbus"
NS_CTXDESC = NS_CTXBROKER + "/ctxdescription"


class NimbusClusterDocument(object):
    """Parse a Nimbus 'cluster document' to
    obtain all need information to create all
    Nodes for a given Cluster.
    """

    public_nic_prefix = None
    local_nic_prefix = None

    def __init__(self, doc, public_nic_prefix="public", local_nic_prefix="private"):
        # these are ugly. Used in nic matching process; they must
        # be in place before parse.
        self.public_nic_prefix = public_nic_prefix
        self.local_nic_prefix = local_nic_prefix
        self.members = []
        self.parse(doc)

    def parse(self, doc):
        """The XML parsing logic.

        """
        self.tree = ET.fromstring(doc)

        if self.tree.tag != 'cluster':
            raise ValidationError("Root element must be 'cluster'")

        members = self.tree.findall('workspace')
        if members is None:
            raise ValidationError("Must have at least one 'workspace' element")

        self.members = [_ClusterMember(self, node) for node in members]

        self.needs_contextualization = False
        for member in self.members:
            if member.needs_contextualization:
                self.needs_contextualization = True
                break

        # we must namespace-prefix all elements, to stay friendly with how
        # the ctx agent parses. It would be more efficent to do this and the
        # above parsing activities in one pass..
        for child in self.tree.getiterator():
            if str(child.tag)[0] != '{':
                child.tag = _ctx_qname(child.tag)

    def build_specs(self, context):
        """
        Produces userdata launch information for cluster document, using the
        specified context.
        """

        if context:
            ctx_tree = ET.Element('NIMBUS_CTX')
            ctx_tree.append(create_contact_element(context))
            ctx_tree.append(self.tree)

        specs = []
        for member in self.members:
            userdata = None
            if member.needs_contextualization:
                member.set_active_state(True)
                userdata = ET.tostring(ctx_tree)
                member.set_active_state(False)
            s = ClusterNodeSpec(image=member.image, count=member.quantity,
                name=member.name, userdata=userdata)
            specs.append(s)
        return specs


def create_contact_element(context):
    """
    Produces a <contact> element for a Context resource
    """
    elem = ET.Element(_ctx_qname('contact'))
    ET.SubElement(elem, _ctx_qname('brokerURL')).text = context['broker_uri']
    ET.SubElement(elem, _ctx_qname('contextID')).text = context['context_id']
    ET.SubElement(elem, _ctx_qname('secret')).text = context['secret']
    return elem


def _ctx_qname(tag):
    return ET.QName(NS_CTXDESC, tag)


class _ClusterMember(object):
    """
    A single 'workspace' of a cluster document.

    XXX: Instances of this object probably should be read only?
    """

    def __init__(self, document, element):
        self.document = document
        self.element = element

        self.image = _get_one_subelement(element, 'image').text.strip()
        quantity = _get_one_subelement(element, 'quantity')
        try:
            self.quantity = int(quantity.text)
        except ValueError:
            raise ValidationError("Workspace quantity must be an integer")
        nameElem = element.find('name')
        if nameElem is not None and nameElem.text:
            self.name = nameElem.text.strip()
        else:
            self.name = ''

        # TODO validate NICs/doctor ctx

        if element.find('active') is not None:
            raise ValidationError("Workspace may not have an 'active' element")
        self._active_element = ET.SubElement(element, 'active')
        self._active_element.text = 'false'

        # TODO determine this value for reals
        self.needs_contextualization = True

    def set_active_state(self, state):
        self._active_element.text = state and 'true' or 'false'


def _get_one_subelement(element, tag):
    result = element.findall(tag)
    if result is None or len(result) != 1:
        raise ValidationError(
            "There must be exactly one '%s' element per workspace" % tag)
    if not result[0].text.strip():
        raise ValidationError("The '%s' element must have a value" % tag)
    return result[0]


class ValidationError(Exception):
    """
    Problem validating structure of cluster document.
    """
    def __init(self, reason):
        self.reason = reason
        Exception.__init__(self, reason)


class ClusterNodeSpec(object):
    """
    Information needed to launch a single cluster member.
    Image name (ami), node count, and userdata.
    """

    def __init__(self, image=None, count='1', name=None, size="m1.small",
                 userdata=None, keyname=None):
        self.image = image
        self.count = count
        self.name = name  # XXX how to specify?
        self.size = size  # XXX how to specify?
        self.userdata = userdata
        self.keyname = keyname
