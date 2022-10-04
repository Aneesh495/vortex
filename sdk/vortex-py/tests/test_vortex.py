# Copyright (c) Vortex Systems. All rights reserved.
# Licensed under the MIT License.

import unittest
from vortex.protocol import WireFrame, ApiKey, VortexError, Record, RecordBatch
from vortex.producer import VortexProducer, Partitioner
from vortex.consumer import VortexConsumer, ConsumerRecord
from vortex.admin import VortexAdmin

class TestVortexPythonSdk(unittest.TestCase):
    def test_wire_frame_round_trip(self):
        payload = b"test-payload-bytes"
        frame = WireFrame(
            api_key=ApiKey.PRODUCE,
            api_version=2,
            correlation_id=101,
            client_id="vortex-py-client",
            payload=payload,
        )

        encoded = frame.encode()
        self.assertGreater(len(encoded), len(payload))

        decoded = WireFrame.decode(encoded)
        self.assertEqual(decoded.api_key, ApiKey.PRODUCE)
        self.assertEqual(decoded.api_version, 2)
        self.assertEqual(decoded.correlation_id, 101)
        self.assertEqual(decoded.client_id, "vortex-py-client")
        self.assertEqual(decoded.payload, payload)

    def test_partitioner_deterministic_hash(self):
        partitioner = Partitioner()
        key = b"account-99124"
        p1 = partitioner.partition(key, 8)
        p2 = partitioner.partition(key, 8)
        p3 = partitioner.partition(key, 8)
        self.assertEqual(p1, p2)
        self.assertEqual(p2, p3)
        self.assertTrue(0 <= p1 < 8)

    def test_producer_send_and_drain(self):
        producer = VortexProducer()
        producer.register_topic_metadata("metrics", 4)

        meta1 = producer.send("metrics", b"sensor-1", b"temp=21.4")
        meta2 = producer.send_to_partition("metrics", 2, None, b"ping")

        self.assertEqual(meta1.topic, "metrics")
        self.assertTrue(0 <= meta1.partition < 4)
        self.assertEqual(meta1.offset, 0)

        self.assertEqual(meta2.topic, "metrics")
        self.assertEqual(meta2.partition, 2)

        drained = producer.drain_batches()
        self.assertGreater(len(drained), 0)
        self.assertEqual(len(producer.accumulator), 0)

    def test_consumer_poll_and_commit(self):
        consumer = VortexConsumer()
        tp0 = ("orders", 0)
        consumer.assign([tp0])

        r1 = ConsumerRecord(topic="orders", partition=0, offset=0, timestamp=1000, value=b"order-1")
        r2 = ConsumerRecord(topic="orders", partition=0, offset=1, timestamp=1010, value=b"order-2")
        consumer.buffer_records([r1, r2])

        polled = consumer.poll(100)
        self.assertEqual(len(polled), 2)
        self.assertEqual(polled[0].value_str(), "order-1")
        self.assertEqual(polled[1].value_str(), "order-2")

        self.assertEqual(consumer.position("orders", 0), 2)
        self.assertEqual(consumer.committed("orders", 0), -1)

        consumer.commit_sync()
        self.assertEqual(consumer.committed("orders", 0), 2)

        consumer.seek("orders", 0, 10)
        self.assertEqual(consumer.position("orders", 0), 10)

    def test_admin_create_and_describe_topic(self):
        admin = VortexAdmin()
        res1 = admin.create_topic("payments", 12, 3)
        self.assertEqual(res1, VortexError.NONE)

        res_dup = admin.create_topic("payments", 12, 3)
        self.assertEqual(res_dup, VortexError.TOPIC_ALREADY_EXISTS)

        topics = admin.list_topics()
        self.assertIn("payments", topics)

        desc = admin.describe_topic("payments")
        self.assertIsNotNone(desc)
        self.assertEqual(desc.name, "payments")
        self.assertEqual(desc.partition_count, 12)
        self.assertEqual(desc.replication_factor, 3)

        del_res = admin.delete_topic("payments")
        self.assertEqual(del_res, VortexError.NONE)
        self.assertNotIn("payments", admin.list_topics())

if __name__ == "__main__":
    unittest.main()
