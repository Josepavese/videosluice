# ADR 0001: application-independent product boundaries

The product owns source admission, media workers, frame access and routing.
Applications own configuration translation, supervision, semantic processing and
system deployment. Mixing their fixture imports and service names into the
product made standalone verification incomplete.

Camera admission attests the preferred physical identity for every selector,
including arbitrary device aliases. Path spelling never selects a security mode.
Missing preferred identity and foreign physical identities remain fail-closed.
No configuration schema, wire API, stored data or device identity is changed.

Application integration and exclusive-camera service guards are maintained by
applications and staged only into their private test workspaces. Product tests
retain source identity, worker, SDK and native boundary contracts. Operational
history remains preserved by its owner; product documentation states current
contracts and measured limitations.

Approved external Go dependencies are explicit in scripts/check-boundaries.py;
adding one requires review of purpose, license and boundary. Production domains
and providers retain their existing AST/native isolation tests. This avoids
maintaining an application-name denylist inside the product itself.
