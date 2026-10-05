from __future__ import annotations

import argparse
import asyncio
import logging
import math
import resource
import time
from array import array
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from aiokafka import AIOKafkaProducer

from benchmarking.generator import generated_frames
from benchmarking.results import BenchmarkResult
from ingestion.logging import configure_logging
from ingestion.sources.parsing import parse_v2


@dataclass(frozen=True, slots=True)
class Scenario:
    count: int
    rate: int | None
    duplicate_fraction: float
    target_run: bool


SCENARIOS = {
    "smoke": Scenario(10_000, None, 0.10, False),
    "sustained": Scenario(1_800_000, 1_000, 0.0, True),
    "burst": Scenario(1_500_000, 5_000, 0.0, True),
    "replay": Scenario(5_000_000, None, 0.0, True),
    "duplicates": Scenario(1_000_000, None, 0.10, True),
}


def percentile(values: array[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = max(0, math.ceil(quantile * len(ordered)) - 1)
    return float(ordered[position])


async def _publish_batch(
    producer: AIOKafkaProducer,
    topic: str,
    pending: list[tuple[bytes, bytes, float]],
    latencies: array[float],
) -> None:
    futures = [await producer.send(topic, value=value, key=key) for value, key, _ in pending]
    await asyncio.gather(*futures)
    acknowledged_at = time.perf_counter()
    latencies.extend((acknowledged_at - started) * 1000 for _, _, started in pending)


async def execute(
    scenario: Scenario,
    *,
    transport: str,
    bootstrap_servers: str,
    topic: str,
    batch_size: int,
) -> dict[str, Any]:
    logger = logging.getLogger("benchmarking.run")
    latencies = array("d")
    previous_event_id: str | None = None
    unique_count = 0
    duplicate_count = 0
    errors = 0
    total_bytes = 0
    started = time.perf_counter()
    cpu_started = time.process_time()
    next_deadline = started
    producer: AIOKafkaProducer | None = None
    pending: list[tuple[bytes, bytes, float]] = []
    if transport == "kafka":
        producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            acks="all",
            enable_idempotence=True,
            compression_type="gzip",
            client_id="atmospulse-benchmark",
        )
        await producer.start()
    try:
        for index, raw in enumerate(
            generated_frames(scenario.count, duplicate_fraction=scenario.duplicate_fraction),
            start=1,
        ):
            item_started = time.perf_counter()
            try:
                event, _ = parse_v2(raw, "benchmark://generated")
                if event.event_id == previous_event_id:
                    duplicate_count += 1
                else:
                    unique_count += 1
                previous_event_id = event.event_id
                total_bytes += len(raw)
                if producer is not None:
                    pending.append((event.kafka_value(), event.did.encode(), item_started))
                    if len(pending) >= batch_size:
                        await _publish_batch(producer, topic, pending, latencies)
                        pending.clear()
                else:
                    latencies.append((time.perf_counter() - item_started) * 1000)
            except Exception:
                errors += 1
                logger.exception("benchmark_event_failed", extra={"fields": {"index": index}})
            if scenario.rate:
                next_deadline += 1 / scenario.rate
                delay = next_deadline - time.perf_counter()
                if delay > 0:
                    await asyncio.sleep(delay)
            if index % 100_000 == 0:
                logger.info(
                    "benchmark_progress",
                    extra={"fields": {"processed": index, "target": scenario.count}},
                )
        if producer is not None and pending:
            await _publish_batch(producer, topic, pending, latencies)
    finally:
        if producer is not None:
            await producer.stop()
    elapsed = time.perf_counter() - started
    usage = resource.getrusage(resource.RUSAGE_SELF)
    duplicate_rate = duplicate_count / scenario.count
    return {
        "event_count": scenario.count,
        "input_bytes": total_bytes,
        "elapsed_seconds": elapsed,
        "throughput_events_per_second": scenario.count / elapsed,
        "latency_ms_p50": percentile(latencies, 0.50),
        "latency_ms_p95": percentile(latencies, 0.95),
        "latency_ms_p99": percentile(latencies, 0.99),
        "cpu_seconds": time.process_time() - cpu_started,
        "max_rss_platform_units": usage.ru_maxrss,
        "error_count": errors,
        "duplicate_count": duplicate_count,
        "duplicate_rate": duplicate_rate,
        "canonical_unique_count": unique_count,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a versioned AtmosPulse event benchmark")
    parser.add_argument("scenario", choices=sorted(SCENARIOS))
    parser.add_argument("--transport", choices=("memory", "kafka"), default="memory")
    parser.add_argument("--bootstrap-servers", default="localhost:19092")
    parser.add_argument("--topic", default="bluesky.raw.events")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--output-dir", type=Path, default=Path("benchmark/results"))
    args = parser.parse_args()
    configure_logging()
    scenario = SCENARIOS[args.scenario]
    measurements = asyncio.run(
        execute(
            scenario,
            transport=args.transport,
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            batch_size=args.batch_size,
        )
    )
    result = BenchmarkResult.create(
        benchmark_name=f"{args.scenario}-{args.transport}",
        parameters={
            "count": scenario.count,
            "requested_rate": scenario.rate,
            "duplicate_fraction": scenario.duplicate_fraction,
            "transport": args.transport,
            "batch_size": args.batch_size,
        },
        measurements=measurements,
        method=(
            "Generated Jetstream v2 frames -> contract parser -> deterministic identity "
            "-> selected transport"
        ),
        target_run=scenario.target_run,
    )
    path = result.write(args.output_dir)
    logging.getLogger(__name__).info(
        "benchmark_complete",
        extra={"fields": {"result": str(path), "status": result.status}},
    )
    print(path)
    return 0 if measurements["error_count"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
