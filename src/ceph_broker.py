#!/usr/bin/env python3

# Copyright 2023 Canonical Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""Handle Ceph Broker requests.

The code is taken from below repo and changed bit to suit the needs of charm.
https://opendev.org/openstack/charms.ceph/src/branch/master/charms_ceph/broker.py
"""

import collections
import functools
import json
import socket
from subprocess import CalledProcessError, check_call, check_output

from ceph import (
    DEBUG,
    ERROR,
    INFO,
    ErasurePool,
    ReplicatedPool,
    erasure_profile_exists,
    get_osds,
    log,
    monitor_key_get,
    monitor_key_set,
    pool_exists,
)

VAR_LIB_CEPH = "/var/snap/microceph/common/data"


def decode_req_encode_rsp(f):
    """Decorator to decode incoming requests and encode responses."""

    def decode_inner(req):
        return json.dumps(f(json.loads(req)))

    return decode_inner


def get_group_key(group_name):
    """Build group key."""
    return "cephx.groups.{}".format(group_name)


def get_group(group_name):
    """Get group based on group_name.

    A group is a structure to hold data about a named group, structured as:
    {
        pools: ['glance'],
        services: ['nova']
    }
    """
    group_key = get_group_key(group_name=group_name)
    group_json = monitor_key_get(service="admin", key=group_key)
    try:
        group = json.loads(group_json)
    except (TypeError, ValueError):
        group = None
    if not group:
        group = {"pools": [], "services": []}
    return group


def save_group(group, group_name):
    """Persist a group in the monitor cluster."""
    group_key = get_group_key(group_name=group_name)
    return monitor_key_set(service="admin", key=group_key, value=json.dumps(group, sort_keys=True))


def _build_service_groups(service, namespace=None):
    """Rebuild the 'groups' dict for a service group.

    :returns: dict: dictionary keyed by group name of the following
                    format:

                    {
                        'images': {
                            pools: ['glance'],
                            services: ['nova', 'glance]
                         },
                         'vms':{
                            pools: ['nova'],
                            services: ['nova']
                         }
                    }
    """
    all_groups = {}
    for groups in service["group_names"].values():
        for group in groups:
            name = group
            if namespace:
                name = "{}-{}".format(namespace, name)
            all_groups[group] = get_group(group_name=name)
    return all_groups


def get_service_groups(service, namespace=None):
    """Get service groups based on service name.

    Services are objects stored with some metadata, they look like (for a
    service named "nova"):
    {
        group_names: {'rwx': ['images']},
        groups: {}
    }
    After populating the group, it looks like:
    {
        group_names: {'rwx': ['images']},
        groups: {
            'images': {
                pools: ['glance'],
                services: ['nova']
            }
        }
    }
    """
    service_json = monitor_key_get(service="admin", key="cephx.services.{}".format(service))
    try:
        service = json.loads(service_json)
    except (TypeError, ValueError):
        service = None
    if service:
        service["groups"] = _build_service_groups(service, namespace)
    else:
        service = {"group_names": {}, "groups": {}}
    return service


def pool_permission_list_for_service(service):
    """Build the permission string for Ceph for a given service."""
    permissions = []
    permission_types = collections.OrderedDict()
    for permission, group in sorted(service["group_names"].items()):
        if permission not in permission_types:
            permission_types[permission] = []
        for item in group:
            permission_types[permission].append(item)
    for permission, groups in permission_types.items():
        permission = "allow {}".format(permission)
        for group in groups:
            for pool in service["groups"][group].get("pools", []):
                permissions.append("{} pool={}".format(permission, pool))
    for permission, prefixes in sorted(service.get("object_prefix_perms", {}).items()):
        for prefix in prefixes:
            permissions.append("allow {} object_prefix {}".format(permission, prefix))
    return [
        "mon",
        ('allow r, allow command "osd blacklist"' ', allow command "osd blocklist"'),
        "osd",
        ", ".join(permissions),
    ]


def update_service_permissions(service, service_obj=None, namespace=None):
    """Update the key permissions for the named client in Ceph."""
    if not service_obj:
        service_obj = get_service_groups(service=service, namespace=namespace)
    permissions = pool_permission_list_for_service(service_obj)
    call = ["ceph", "auth", "caps", "client.{}".format(service)] + permissions
    try:
        check_call(call)
    except CalledProcessError as e:
        log("Error updating key capabilities: {}".format(e))


def add_pool_to_group(pool, group, namespace=None):
    """Add a named pool to a named group."""
    group_name = group
    if namespace:
        group_name = "{}-{}".format(namespace, group_name)
    group = get_group(group_name=group_name)
    if pool not in group["pools"]:
        group["pools"].append(pool)
    save_group(group, group_name=group_name)
    for service in group["services"]:
        update_service_permissions(service, namespace=namespace)


@decode_req_encode_rsp
def process_requests(reqs):
    """Process Ceph broker request(s).

    This is a versioned api. API version must be supplied by the client making
    the request.

    :param reqs: dict of request parameters.
    :returns: dict. exit-code and reason if not 0
    """
    request_id = reqs.get("request-id")
    try:
        version = reqs.get("api-version")
        if version == 1:
            log("Processing request {}".format(request_id), level=DEBUG)
            resp = process_requests_v1(reqs["ops"])
            if request_id:
                resp["request-id"] = request_id

            return resp

    except Exception as exc:
        log(str(exc), level=ERROR)
        msg = "Unexpected error occurred while processing requests: %s" % reqs
        log(msg, level=ERROR)
        return {"exit-code": 1, "stderr": msg}

    msg = "Missing or invalid api version ({})".format(version)
    resp = {"exit-code": 1, "stderr": msg}
    if request_id:
        resp["request-id"] = request_id

    return resp


def process_requests_v1(reqs):
    """Process v1 requests.

    Takes a list of requests (dicts) and processes each one. If an error is
    found, processing stops and the client is notified in the response.

    Returns a response dict containing the exit code (non-zero if any
    operation failed along with an explanation).
    """
    ret = None
    log("Processing {} ceph broker requests".format(len(reqs)), level=INFO)
    for req in reqs:
        op = req.get("op")
        log("Processing op='{}'".format(op), level=DEBUG)
        # Use admin client since we do not have other client key locations
        # setup to use them for these operations.
        svc = "admin"
        if op == "create-pool":
            pool_type = req.get("pool-type")  # "replicated" | "erasure"

            # Default to replicated if pool_type isn't given
            if pool_type == "erasure":
                ret = handle_erasure_pool(request=req, service=svc)
            else:
                ret = handle_replicated_pool(request=req, service=svc)
        else:
            msg = "Unknown operation '{}'".format(op)
            log(msg, level=ERROR)
            return {"exit-code": 1, "stderr": msg}

    if type(ret) == dict and "exit-code" in ret:
        return ret

    return {"exit-code": 0}


def handle_erasure_pool(request, service):
    """Create a new erasure coded pool.

    :param request: dict of request operations and params.
    :param service: The ceph client to run the command under.
    :returns: dict. exit-code and reason if not 0.
    """
    pool_name = request.get("name")
    erasure_profile = request.get("erasure-profile")
    group_name = request.get("group")

    if erasure_profile is None:
        erasure_profile = "default-canonical"

    if group_name:
        group_namespace = request.get("group-namespace")
        # Add the pool to the group named "group_name"
        add_pool_to_group(pool=pool_name, group=group_name, namespace=group_namespace)

    # TODO: Default to 3/2 erasure coding. I believe this requires min 5 osds
    if not erasure_profile_exists(service=service, name=erasure_profile):
        # TODO: Fail and tell them to create the profile or default
        msg = (
            "erasure-profile {} does not exist.  Please create it with: "
            "create-erasure-profile".format(erasure_profile)
        )
        log(msg, level=ERROR)
        return {"exit-code": 1, "stderr": msg}

    try:
        pool = ErasurePool(service=service, op=request)
    except KeyError:
        msg = "Missing parameter."
        log(msg, level=ERROR)
        return {"exit-code": 1, "stderr": msg}

    # Ok make the erasure pool
    if not pool_exists(service=service, name=pool_name):
        log(
            "Creating pool '{}' (erasure_profile={})".format(pool.name, erasure_profile),
            level=INFO,
        )
        pool.create()

    # Set/update properties that are allowed to change after pool creation.
    pool.update()


def handle_replicated_pool(request, service):
    """Create a new replicated pool.

    :param request: dict of request operations and params.
    :param service: The ceph client to run the command under.
    :returns: dict. exit-code and reason if not 0.
    """
    pool_name = request.get("name")
    group_name = request.get("group")

    # Optional params
    # NOTE: Check this against the handling in the Pool classes, reconcile and
    # remove.
    pg_num = request.get("pg_num")
    replicas = request.get("replicas")
    if pg_num:
        # Cap pg_num to max allowed just in case.
        osds = get_osds(service)
        if osds:
            pg_num = min(pg_num, (len(osds) * 100 // replicas))
            request.update({"pg_num": pg_num})

    if group_name:
        group_namespace = request.get("group-namespace")
        # Add the pool to the group named "group_name"
        add_pool_to_group(pool=pool_name, group=group_name, namespace=group_namespace)

    try:
        pool = ReplicatedPool(service=service, op=request)
    except KeyError:
        msg = "Missing parameter."
        log(msg, level=ERROR)
        return {"exit-code": 1, "stderr": msg}

    if not pool_exists(service=service, name=pool_name):
        log("Creating pool '{}' (replicas={})".format(pool.name, replicas), level=INFO)
        pool.create()
    else:
        log("Pool '{}' already exists - skipping create".format(pool.name), level=DEBUG)

    # Set/update properties that are allowed to change after pool creation.
    pool.update()


# Below functions are picked from
# https://opendev.org/openstack/charms.ceph/src/branch/master/charms_ceph/utils.py

LEADER = "leader"

_default_caps = collections.OrderedDict(
    [
        ("mon", ["allow r", 'allow command "osd blacklist"', 'allow command "osd blocklist"']),
        ("osd", ["allow rwx"]),
    ]
)


def parse_key(raw_key):
    """Parse the key."""
    # get-or-create appears to have different output depending
    # on whether its 'get' or 'create'
    # 'create' just returns the key, 'get' is more verbose and
    # needs parsing
    key = None
    if len(raw_key.splitlines()) == 1:
        key = raw_key
    else:
        for element in raw_key.splitlines():
            if "key" in element:
                return element.split(" = ")[1].strip()  # IGNORE:E1103
    return key


@functools.lru_cache()
def ceph_auth_get(key_name):
    """Get ceph auth key."""
    try:
        # Does the key already exist?
        output = str(
            check_output(
                [
                    "ceph",
                    "--name",
                    "mon.",
                    "--keyring",
                    f"{VAR_LIB_CEPH}/mon/ceph-{socket.gethostname()}/keyring",
                    "auth",
                    "get",
                    key_name,
                ]
            ).decode("UTF-8")
        ).strip()
        return parse_key(output)
    except CalledProcessError:
        # Couldn't get the key
        pass


def get_named_key(name, caps=None, pool_list=None):
    """Retrieve a specific named cephx key.

    :param name: String Name of key to get.
    :param pool_list: The list of pools to give access to
    :param caps: dict of cephx capabilities
    :returns: Returns a cephx key
    """
    key_name = "client.{}".format(name)
    key = ceph_auth_get(key_name)
    if key:
        return key

    log("Creating new key for {}".format(name), level=DEBUG)
    caps = caps or _default_caps
    cmd = [
        "ceph",
        "--name",
        "mon.",
        "--keyring",
        f"{VAR_LIB_CEPH}/mon/ceph-{socket.gethostname()}/keyring",
        "auth",
        "get-or-create",
        key_name,
    ]
    # Add capabilities
    for subsystem, subcaps in caps.items():
        if subsystem == "osd":
            if pool_list:
                # This will output a string similar to:
                # "pool=rgw pool=rbd pool=something"
                pools = " ".join(["pool={0}".format(i) for i in pool_list])
                subcaps[0] = subcaps[0] + " " + pools
        cmd.extend([subsystem, "; ".join(subcaps)])
    ceph_auth_get.cache_clear()

    log("Calling check_output: {}".format(cmd), level=DEBUG)
    return parse_key(str(check_output(cmd).decode("UTF-8")).strip())  # IGNORE:E1103


def is_leader():
    """Check if this node is ceph mon leader."""
    hostname = socket.gethostname()
    cmd = ["ceph", "tell", f"mon.{hostname}", "mon_status", "--format", "json"]
    try:
        result = json.loads(str(check_output(cmd).decode("UTF-8")))
    except CalledProcessError:
        return False
    except ValueError:
        # Non JSON response from mon_status
        return False

    if result["state"] == LEADER:
        return True
    else:
        return False
