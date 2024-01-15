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


"""Microceph Operator Charm.

This charm deploys and manages microceph.
"""
import json
import logging
from typing import Callable

from ops.charm import CharmBase, RelationEvent
from ops.framework import EventBase, EventSource, Object, ObjectEvents, StoredState
from ops_sunbeam.interfaces import OperatorPeers
from ops_sunbeam.relation_handlers import BasePeerHandler, RelationHandler

from ceph_broker import is_leader as is_ceph_mon_leader
from ceph_broker import process_requests

logger = logging.getLogger(__name__)


class MicroClusterNewNodeEvent(RelationEvent):
    """charm runs add-node in response to this event, passes join URL back."""


class MicroClusterNodeAddedEvent(RelationEvent):
    """charm runs join in response to this event using supplied join URL."""


class MicroClusterRemoveNodeEvent(RelationEvent):
    """charm runs remove-node to this event."""


class MicroClusterEvents(ObjectEvents):
    """Events related to MicroCluster apps."""

    add_node = EventSource(MicroClusterNewNodeEvent)
    node_added = EventSource(MicroClusterNodeAddedEvent)
    remove_node = EventSource(MicroClusterRemoveNodeEvent)


class MicroClusterPeers(OperatorPeers):
    """Interface for the microcluster peers relation."""

    on = MicroClusterEvents()

    def __init__(self, charm: CharmBase, relation_name: str) -> None:
        """Run constructor."""
        super().__init__(charm, relation_name)

        self.state.set_default(joined=False)
        self.framework.observe(charm.on[relation_name].relation_departed, self.on_departed)

    def _event_args(self, relation_event):
        return dict(
            relation=relation_event.relation,
            app=relation_event.app,
            unit=relation_event.unit,
        )

    def on_created(self, event: EventBase) -> None:
        """Handle relation created event."""
        # Do nothing or raise an event to charm?
        pass

    def on_changed(self, event: EventBase) -> None:
        """Handle relation changed event."""
        keys = [key for key in self.get_all_app_data().keys() if key.endswith(".join_token")]
        if event.unit and self.model.unit.is_leader():
            if not keys:
                logger.debug("We are the seed node.")
                # The seed node is implicitly joined, so there's no need to emit an event.
                self.state.joined = True

            if f"{event.unit.name}.join_token" in keys:
                logger.debug(f"Already added {event.unit.name} to the cluster")
                return

            logger.debug("Emitting add_node event")
            self.on.add_node.emit(**self._event_args(event))
        else:
            # Node already joined as member of cluster
            if self.state.joined:
                logger.debug(f"Node {self.model.unit.name} already joined")
                return

            # Join token not yet generated for this node
            if f"{self.model.unit.name}.join_token" not in keys:
                logger.debug(f"Join token not yet generated for node {self.model.unit.name}")
                return

            # TOCHK: Can we pull app data and unit data and emit node_added events based on them
            # do we need to save joined in unit data which might trigger relation-changed event?
            logger.debug("Emitting node_added event")
            event_args = self._event_args(event)
            event_args["unit"] = self.model.unit
            self.on.node_added.emit(**event_args)

    def on_joined(self, event: EventBase) -> None:
        """Handle relation joined event."""
        # Do nothing or raise an event to charm?
        pass

    def on_departed(self, event: EventBase) -> None:
        """Handle relation departed event."""
        if not event.unit:
            return

        if not self.model.unit.is_leader():
            return

        # TOCHK: Can we remove node which is not joined?
        logger.debug("Emitting remove_unit event")
        self.on.remove_node.emit(**self._event_args(event))


class MicroClusterPeerHandler(BasePeerHandler):
    """Base handler for managing a peers relation."""

    def setup_event_handler(self) -> None:
        """Configure event handlers for peer relation."""
        logger.debug("Setting up peer event handler")
        peer_int = MicroClusterPeers(self.charm, self.relation_name)

        self.framework.observe(peer_int.on.add_node, self._on_add_node)
        self.framework.observe(peer_int.on.node_added, self._on_node_added)
        self.framework.observe(peer_int.on.remove_node, self._on_remove_node)

        return peer_int

    def _on_add_node(self, event):
        if not self.is_leader_ready():
            logger.debug("Add node event, deferring the event as leader not ready")
            event.defer()
            return

        if not self.model.unit.is_leader():
            logger.debug("Ignoring Add node event as this is not leader unit")
            return

        self.callback_f(event)
        # The event is emitted only onto leader node or this node
        # self.interface.on.node_added.emit(**self.interface._event_args(event))

    def _on_node_added(self, event):
        if self.model.unit.name != event.unit.name:
            logger.debug("Ignoring Node Added event, event received on other node")
            return

        self.callback_f(event)

    def _on_remove_node(self, event):
        if not self.is_leader_ready():
            logger.debug("Remove node event, deferring the event as leader not ready")
            event.defer()
            return

        if not self.model.unit.is_leader():
            logger.debug("Ignoring Remove node event as this is not leader unit")
            return

        self.callback_f(event)


class ProcessBrokerRequestEvent(EventBase):
    """Event to process a ceph broker request."""

    def __init__(
        self,
        handle,
        relation_id,
        relation_name,
        broker_req_id,
        broker_req,
        client_app_name,
        client_unit_name,
    ):
        super().__init__(handle)
        self.relation_id = relation_id
        self.relation_name = relation_name
        self.broker_req_id = broker_req_id
        self.broker_req = broker_req
        self.client_app_name = client_app_name
        self.client_unit_name = client_unit_name

    def snapshot(self):
        """Snapshot the event data."""
        return {
            "relation_id": self.relation_id,
            "relation_name": self.relation_name,
            "broker_req_id": self.broker_req_id,
            "broker_req": self.broker_req,
            "client_app_name": self.client_app_name,
            "client_unit_name": self.client_unit_name,
        }

    def restore(self, snapshot):
        """Restore the event data."""
        super().restore(snapshot)
        self.relation_id = snapshot["relation_id"]
        self.relation_name = snapshot["relation_name"]
        self.broker_req_id = snapshot["broker_req_id"]
        self.broker_req = snapshot["broker_req"]
        self.client_app_name = snapshot["client_app_name"]
        self.client_unit_name = snapshot["client_unit_name"]


class CephClientProviderEvents(ObjectEvents):
    """Define all CephClient provider events."""

    process_request = EventSource(ProcessBrokerRequestEvent)


class CephClientProvides(Object):
    """Interface for cephclient provider."""

    on = CephClientProviderEvents()
    _stored = StoredState()

    def __init__(self, charm, relation_name="ceph"):
        super().__init__(charm, relation_name)

        self._stored.set_default(processed=[])
        self.charm = charm
        self.this_unit = self.model.unit
        self.relation_name = relation_name
        self.framework.observe(
            charm.on[self.relation_name].relation_joined, self._on_relation_changed
        )
        self.framework.observe(
            charm.on[self.relation_name].relation_changed, self._on_relation_changed
        )

    def _on_relation_changed(self, event):
        """Prepare relation for data from requiring side."""
        # send_osd_settings()
        logger.info("_on_relation_changed event")

        if not self.charm.ready_for_service():
            logger.info("Not processing request as service is not yet ready")
            return

        self._handle_client_relation(event.relation, event.unit)

    def _get_client_application_name(self, relation, unit):
        """Retrieve client application name from relation data."""
        return relation.data[unit].get("application-name", relation.app.name)

    def _req_already_treated(self, request_id, relation, req_unit):
        """Check if broker request already handled.

        The local relation data holds all the broker request/responses that
        are handled as a dictionary. There will be a single entry for each
        unit that makes broker request in the form of broker-rsp-<unit name>:
        {reqeust-id: <id>, ..}. Verify if request_id exists in the relation
        data broker response for the requested unit.

        :param request_id: Request ID
        :type request_id: str
        :param relation: Operator relation
        :type relation: Relation
        :param unit: Unit to handle
        :type unit: Unit
        :returns: Whether request is already handled
        :rtype: bool
        """
        status = relation.data[req_unit]
        client_unit_name = status.get("unit-name", req_unit.name)
        response_key = "broker-rsp-" + client_unit_name.replace("/", "-")
        if not status.get(response_key):
            return False

        if isinstance(status[response_key], str):
            try:
                data = json.loads(status[response_key])
            except (TypeError, json.decoder.JSONDecodeError):
                logging.debug(f"Not able to decode broker response {req_unit}")
                return False
        else:
            data = status[response_key]

        return data.get("request-id") == request_id

    def _get_broker_req_id(self, request):
        try:
            if isinstance(request, str):
                try:
                    req_key = json.loads(request)["request-id"]
                except (TypeError, json.decoder.JSONDecodeError):
                    logger.warning(
                        "Not able to decode request " "id for broker request {}".format(request)
                    )
                    req_key = None
            else:
                req_key = request["request-id"]
        except KeyError:
            logger.warning("Not able to decode request id for broker request {}".format(request))
            req_key = None

        return req_key

    def _handle_client_relation(self, relation, unit):
        """Handle broker request and set the relation data.

        :param relation: Operator relation
        :type relation: Relation
        :param unit: Unit to handle
        :type unit: Unit
        """
        logger.info(
            "mon cluster in quorum and osds bootstrapped "
            "- providing client with keys, processing broker requests"
        )

        settings = relation.data[unit]
        if "broker_req" not in settings:
            logger.warning(f"broker_req not in settings: {settings}")
            return

        broker_req_id = self._get_broker_req_id(settings["broker_req"])
        if broker_req_id is None:
            return

        if not is_ceph_mon_leader():
            logger.debug(f"Not leader - ignoring broker request {broker_req_id}")
            return

        if self._req_already_treated(broker_req_id, relation, unit):
            logger.info(f"Ignoring already executed broker request {broker_req_id}")
            return

        client_app_name = self._get_client_application_name(relation, unit)
        client_unit_name = settings.get("unit-name", unit.name).replace("/", "-")
        self.on.process_request.emit(
            relation.id,
            relation.name,
            broker_req_id,
            settings["broker_req"],
            client_app_name,
            client_unit_name,
        )

    def set_broker_response(self, relation_id, relation_name, broker_req_id, response, ceph_info):
        """Set broker response in unit data bag."""
        data = {}

        # ceph_info required: key, auth, ceph-public-address, rbd-features
        data.update(ceph_info)

        if response is not None:
            # response should be in format {broker-rsp-<unit name>: rsp}
            data.update(response)

            processed = self._stored.processed
            processed.append(broker_req_id)
            self._stored.processed = processed

        relation = None
        for rel in self.framework.model.relations[relation_name]:
            if rel.id == relation_id:
                relation = rel

        if not relation:
            # Relation has disappeared so skip send of data
            return

        for k, v in data.items():
            relation.data[self.this_unit][k] = str(v)


class CephClientProviderHandler(RelationHandler):
    """Handler for ceph client relation."""

    def __init__(
        self,
        charm: CharmBase,
        relation_name: str,
        callback_f: Callable,
    ):
        super().__init__(charm, relation_name, callback_f)

    def setup_event_handler(self) -> Object:
        """Configure event handlers for an ceph-client interface."""
        logger.debug("Setting up ceph-client event handler")

        ceph = CephClientProvides(
            self.charm,
            self.relation_name,
        )
        self.framework.observe(ceph.on.process_request, self._on_process_request)
        return ceph

    @property
    def ready(self) -> bool:
        """Check if handler is ready."""
        # TODO: True only when charm completes bootstrapping??
        return True

    def _on_process_request(self, event):
        logger.info(f"Processing broker req {event.broker_req}")
        broker_result = process_requests(event.broker_req)
        logger.info(broker_result)
        unit_response_key = "broker-rsp-" + event.client_unit_name
        response = {unit_response_key: broker_result}
        self.interface.set_broker_response(
            event.relation_id,
            event.relation_name,
            event.broker_req_id,
            response,
            self.charm.get_ceph_info_from_configs(event.client_app_name),
        )
        # Ignore the callback function??
