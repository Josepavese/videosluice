"""Bounded payload-receive diagnostics shared by delivery and exact ROI.

The provider fills caller-owned bytes synchronously. On failure they are not
usable; counters describe progress, never authorization or payload integrity.
"""
from dataclasses import dataclass, field
from .causal_envelope import Invalid


@dataclass
class PayloadProgress:
    received_records: int = 0
    received_wire_bytes: int = 0
    validated_payload_bytes: int = 0

    def validate_complete(self, size: int) -> None:
        if (self.validated_payload_bytes != size or not 0 < self.received_records <= size
                or self.received_wire_bytes != size+44*self.received_records):
            raise Invalid("incomplete frame payload receive")


@dataclass
class DeliveryProgress:
    """One bounded wire transaction; descriptors remain untrusted until parsed."""
    payload: PayloadProgress = field(default_factory=PayloadProgress)
    metadata_bytes: int = 0
    header_bytes: int = 0
    declared_payload_bytes: int = 0
    # Native wall time in fixed, non-overlapping stages. Values are diagnostic
    # only; the total transaction deadline and ownership remain authoritative.
    native_send_us: int = 0
    native_descriptor_us: int = 0
    native_payload_receive_us: int = 0
    native_integrity_us: int = 0
    # Native entry/completion and completion-to-Python spans. A negative value
    # means unavailable, never zero wait. The latter is NOT a measured GIL wait.
    native_call_us: int = -1
    native_resume_gap_us: int = -1
    # The last authority check and its return to Python are separate from IO.
    native_authority_call_us: int = -1
    native_authority_cpu_us: int = -1
    native_authority_resume_gap_us: int = -1
    channel_phase: str = "unavailable"
    channel_failure_us: int = -1
    channel_diagnostics_us: int = -1
