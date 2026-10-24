"""
Kademlia routing table and k-buckets implementation.
Handles peer tracking, LRU bucket eviction, and finding closest nodes using XOR distance.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from .node_id import NodeID, NodeContact, DEFAULT_ID_BITS, xor_distance

logger = logging.getLogger("MeshWeaver.DHTRouting")


class KBucket:
    """
    Holds up to k contacts, ordered from least-recently seen (head)
    to most-recently seen (tail).
    """

    def __init__(self, k: int = 20, max_replacement_cache: int = 20) -> None:
        self.k = k
        self.max_replacement_cache = max_replacement_cache
        self._contacts: List[NodeContact] = []
        self._replacement_cache: List[NodeContact] = []
        self.last_updated: float = time.time()

    @property
    def size(self) -> int:
        return len(self._contacts)

    @property
    def is_full(self) -> bool:
        return len(self._contacts) >= self.k

    @property
    def contacts(self) -> List[NodeContact]:
        return list(self._contacts)

    @property
    def replacement_cache(self) -> List[NodeContact]:
        return list(self._replacement_cache)

    @property
    def oldest_contact(self) -> Optional[NodeContact]:
        """Oldest node in bucket (least recently seen). Candidate for ping check if full."""
        return self._contacts[0] if self._contacts else None

    def get_contact(self, node_id: NodeID) -> Optional[NodeContact]:
        for c in self._contacts:
            if c.node_id == node_id:
                return c
        return None

    def add_contact(self, contact: NodeContact) -> Tuple[bool, Optional[NodeContact]]:
        """
        Add or update a contact:
        - If already present: refresh info and move to the end (most recently seen).
        - If space available: append to end.
        - If full: put in replacement cache, return (False, oldest_contact) so caller can ping it.
        """
        self.last_updated = time.time()

        # If already in the bucket, update and move to tail
        for idx, existing in enumerate(self._contacts):
            if existing.node_id == contact.node_id:
                existing.host = contact.host
                existing.port = contact.port
                existing.touch(contact.rtt_ms)
                existing.metadata.update(contact.metadata)
                self._contacts.pop(idx)
                self._contacts.append(existing)
                self._remove_from_cache(contact.node_id)
                return True, None

        # If there's room, simply append
        if len(self._contacts) < self.k:
            contact.touch()
            self._contacts.append(contact)
            self._remove_from_cache(contact.node_id)
            return True, None

        # Bucket is full: buffer in replacement cache
        self._add_to_cache(contact)
        return False, self._contacts[0]

    def touch(self, node_id: NodeID, rtt_ms: Optional[float] = None) -> bool:
        """Move a contact to the end because we just heard from it."""
        for idx, contact in enumerate(self._contacts):
            if contact.node_id == node_id:
                contact.touch(rtt_ms)
                self._contacts.pop(idx)
                self._contacts.append(contact)
                self.last_updated = time.time()
                return True
        return False

    def remove_contact(self, node_id: NodeID) -> Optional[NodeContact]:
        """Remove a contact. If we have a backup in replacement cache, promote it."""
        for idx, contact in enumerate(self._contacts):
            if contact.node_id == node_id:
                removed = self._contacts.pop(idx)
                self.last_updated = time.time()
                if self._replacement_cache:
                    promoted = self._replacement_cache.pop()
                    promoted.touch()
                    self._contacts.append(promoted)
                return removed

        self._remove_from_cache(node_id)
        return None

    def evict_and_replace(self, stale_node_id: NodeID, new_contact: NodeContact) -> bool:
        """Evict an unresponsive node and insert the new one."""
        removed = self.remove_contact(stale_node_id)
        if removed is not None:
            self.add_contact(new_contact)
            return True
        return False

    def _add_to_cache(self, contact: NodeContact) -> None:
        self._remove_from_cache(contact.node_id)
        self._replacement_cache.append(contact)
        if len(self._replacement_cache) > self.max_replacement_cache:
            self._replacement_cache.pop(0)

    def _remove_from_cache(self, node_id: NodeID) -> None:
        self._replacement_cache = [c for c in self._replacement_cache if c.node_id != node_id]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "size": self.size,
            "k": self.k,
            "last_updated": self.last_updated,
            "contacts": [c.to_dict() for c in self._contacts],
            "replacement_count": len(self._replacement_cache),
        }

    def __len__(self) -> int:
        return self.size


class RoutingTable:
    """
    Kademlia routing table containing 160 k-buckets.
    Buckets are indexed by the bit position where peer IDs differ from our own ID.
    """

    def __init__(
        self,
        local_node_id: NodeID,
        k: int = 20,
        num_bits: int = DEFAULT_ID_BITS,
    ) -> None:
        self.local_node_id = local_node_id
        self.k = k
        self.num_bits = num_bits
        self.buckets: List[KBucket] = [KBucket(k=k) for _ in range(num_bits)]

    def add_or_update(self, contact: NodeContact) -> Tuple[bool, Optional[NodeContact]]:
        """Add or update contact in the appropriate bucket."""
        if contact.node_id == self.local_node_id:
            return False, None

        idx = self.local_node_id.bucket_index(contact.node_id)
        return self.buckets[idx].add_contact(contact)

    def remove(self, node_id: NodeID) -> Optional[NodeContact]:
        """Remove a node by its ID."""
        if node_id == self.local_node_id:
            return None
        idx = self.local_node_id.bucket_index(node_id)
        return self.buckets[idx].remove_contact(node_id)

    def touch(self, node_id: NodeID, rtt_ms: Optional[float] = None) -> bool:
        """Mark a node as alive."""
        if node_id == self.local_node_id:
            return False
        idx = self.local_node_id.bucket_index(node_id)
        return self.buckets[idx].touch(node_id, rtt_ms=rtt_ms)

    def get_contact(self, node_id: NodeID) -> Optional[NodeContact]:
        if node_id == self.local_node_id:
            return None
        idx = self.local_node_id.bucket_index(node_id)
        return self.buckets[idx].get_contact(node_id)

    def find_closest_nodes(
        self,
        target_id: NodeID,
        count: Optional[int] = None,
        exclude_ids: Optional[Set[NodeID]] = None,
    ) -> List[NodeContact]:
        """
        Return the `count` closest nodes to target_id, sorted by XOR distance.
        """
        limit = count or self.k
        exclude = set(exclude_ids or set())
        exclude.add(self.local_node_id)

        candidates: List[NodeContact] = []
        for bucket in self.buckets:
            for contact in bucket.contacts:
                if contact.node_id not in exclude:
                    candidates.append(contact)

        # Sort by XOR distance to target
        candidates.sort(key=lambda c: c.node_id.xor_distance(target_id))
        return candidates[:limit]

    @property
    def total_contacts(self) -> int:
        return sum(bucket.size for bucket in self.buckets)

    def get_all_contacts(self) -> List[NodeContact]:
        all_nodes = []
        for b in self.buckets:
            all_nodes.extend(b.contacts)
        return all_nodes

    def get_populated_buckets(self) -> List[Tuple[int, KBucket]]:
        return [(i, b) for i, b in enumerate(self.buckets) if b.size > 0]

    def get_stale_buckets(self, refresh_interval_seconds: float = 300.0) -> List[int]:
        """Find buckets that haven't been touched in a while (useful for periodic refreshes)."""
        cutoff = time.time() - refresh_interval_seconds
        return [i for i, b in enumerate(self.buckets) if b.size > 0 and b.last_updated < cutoff]

    def generate_random_id_for_bucket(self, bucket_idx: int) -> NodeID:
        """
        Generate a random NodeID that falls into bucket_idx relative to our ID.
        Used to refresh buckets by querying for a random key in that range.
        """
        if bucket_idx < 0 or bucket_idx >= self.num_bits:
            raise ValueError(f"Invalid bucket index {bucket_idx}")

        lower = 1 << bucket_idx
        upper = (1 << (bucket_idx + 1)) - 1
        distance = random.randint(lower, upper)
        target_int = self.local_node_id.int_value ^ distance
        return NodeID.from_int(target_int, num_bits=self.num_bits)

    def to_dict(self) -> Dict[str, Any]:
        populated = self.get_populated_buckets()
        return {
            "local_node_id": self.local_node_id.hex_value,
            "total_contacts": self.total_contacts,
            "k": self.k,
            "populated_bucket_count": len(populated),
            "buckets": {f"bucket_{idx}": b.to_dict() for idx, b in populated},
        }

    def summary(self) -> str:
        lines = [f"RoutingTable(Node: {self.local_node_id.short_hex(8)}, Peers: {self.total_contacts})"]
        for idx, bucket in self.get_populated_buckets():
            previews = ", ".join(c.node_id.short_hex(4) for c in bucket.contacts)
            lines.append(f"  Bucket [{idx:3d}]: {bucket.size}/{self.k} -> [{previews}]")
        return "\n".join(lines)
