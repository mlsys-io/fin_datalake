"""
KafkaCollector — Probes Kafka for consumer group lag.

Uses confluent_kafka.AdminClient to check topic offsets
and consumer group positions without consuming any messages.
"""

from __future__ import annotations

from overseer.collectors.base import BaseCollector
from overseer.models import ServiceMetrics


class KafkaCollector(BaseCollector):

    async def collect(self) -> ServiceMetrics:
        try:
            from confluent_kafka.admin import AdminClient, ConsumerGroupTopicPartitions
            from confluent_kafka import TopicPartition

            bootstrap = f"{self.endpoint.host}:{self.endpoint.port}"
            group = self.endpoint.extra.get("consumer_group", "etl-consumer-group")

            admin = AdminClient({"bootstrap.servers": bootstrap})

            # Get committed offsets for the consumer group
            cg = ConsumerGroupTopicPartitions(group)
            offsets_future = admin.list_consumer_group_offsets([cg])
            results = offsets_future[group].result()

            lag_by_topic: dict[str, int] = {}
            for tp in results.topic_partitions:
                topic = tp.topic
                committed = tp.offset if tp.offset >= 0 else 0

                # Get the high watermark (latest offset) for comparison
                # We use a temporary consumer to fetch watermarks
                from confluent_kafka import Consumer
                c = Consumer({
                    "bootstrap.servers": bootstrap,
                    "group.id": "__overseer_probe__",
                    "enable.auto.commit": False,
                })
                low, high = c.get_watermark_offsets(
                    TopicPartition(tp.topic, tp.partition), timeout=5
                )
                c.close()

                lag = max(0, high - committed)
                lag_by_topic[topic] = lag_by_topic.get(topic, 0) + lag

            total_lag = sum(lag_by_topic.values())

            return ServiceMetrics(
                service="kafka",
                healthy=True,
                data={
                    "consumer_group": group,
                    "lag_by_topic": lag_by_topic,
                    "total_lag": total_lag,
                },
            )
        except ImportError:
            return ServiceMetrics(
                service="kafka", healthy=True,
                data={"note": "confluent_kafka not installed — skipping lag check"},
            )
        except Exception as e:
            return ServiceMetrics(service="kafka", healthy=False, error=str(e))

    async def is_healthy(self) -> tuple[bool, str | None]:
        """Kafka doesn't have HTTP health — try connecting via AdminClient."""
        try:
            from confluent_kafka.admin import AdminClient
            admin = AdminClient({
                "bootstrap.servers": f"{self.endpoint.host}:{self.endpoint.port}",
                "socket.timeout.ms": 3000,
            })
            md = admin.list_topics(timeout=3)
            return len(md.topics) >= 0, None
        except Exception as e:
            return False, str(e)
