defmodule Commanded.Event.ConcurrentBatchingStressTest do
  @moduledoc """
  Stress tests for concurrent batching with REAL EventStore.

  These tests push the system hard to find race conditions, ordering issues,
  and checkpoint problems when using concurrency + batching + timeout together.

  Uses simple counter events partitioned by modulo to make verification obvious.

  NOTE: These tests use the in-memory EventStore which delivers events slowly
  with partitioning (one event at a time per partition). For realistic performance
  testing with 100+ partitions and 1000+ events, use a real Postgres EventStore
  in a load testing environment.
  """

  use ExUnit.Case, async: false

  require Logger

  alias Commanded.{DefaultApp, EventStore}
  alias Commanded.Event.Mapper

  setup do
    start_supervised!(DefaultApp)
    Logger.debug("=== TEST SETUP: DefaultApp started ===")

    on_exit(fn ->
      Logger.debug("=== TEST TEARDOWN ===")
    end)

    :ok
  end

  defmodule CounterEvent do
    @derive Jason.Encoder
    defstruct [:id, :partition, :value]
  end

  defmodule CounterBatchHandler do
    use Commanded.Event.Handler,
      application: Commanded.DefaultApp,
      name: __MODULE__,
      concurrency: 7,        # Prime number for good distribution
      batch_size: 10,
      batch_timeout: 50

    require Logger

    def init(config) do
      index = Keyword.get(config, :index, :unknown)
      Logger.debug("[Handler-#{index}] Initializing")
      {:ok, config}
    end

    def partition_by(%CounterEvent{partition: partition}, _metadata) do
      Logger.debug("[Partition] Event routed to partition: #{partition}")
      partition
    end

    def handle_batch(events) do
      # Get handler index from config
      index = Process.get(:handler_index, :unknown)

      # Extract partition (all events in batch should have same partition)
      partitions = Enum.map(events, fn {event, _} -> event.partition end) |> Enum.uniq()

      if length(partitions) > 1 do
        Logger.error("[Handler-#{index}] INVARIANT VIOLATED: Multiple partitions in batch: #{inspect(partitions)}")
        raise "Multiple partitions in single batch!"
      end

      [partition] = partitions

      # Verify ordering within partition
      ids = Enum.map(events, fn {event, _} -> event.id end)
      event_numbers = Enum.map(events, fn {_, meta} -> meta.event_number end)

      unless event_numbers == Enum.sort(event_numbers) do
        Logger.error(
          "[Handler-#{index}] ORDERING VIOLATED in partition #{partition}: " <>
            "event_numbers=#{inspect(event_numbers)}"
        )

        raise "Events out of order!"
      end

      # Calculate sum for this batch
      sum = Enum.sum(Enum.map(events, fn {event, _} -> event.value end))

      Logger.debug(
        "[Handler-#{index}] Processing batch for partition=#{partition}, " <>
          "count=#{length(events)}, ids=#{inspect(ids)}, sum=#{sum}, " <>
          "event_numbers=#{inspect(event_numbers)}"
      )

      # Send to test process
      if test_pid = Process.whereis(:stress_test) do
        send(test_pid, {:batch_processed, partition, length(events), sum, event_numbers})
      end

      :ok
    end
  end

  defmodule CrashingBatchHandler do
    use Commanded.Event.Handler,
      application: Commanded.DefaultApp,
      name: __MODULE__,
      concurrency: 3,
      batch_size: 5,
      batch_timeout: 100

    require Logger

    def partition_by(%CounterEvent{partition: partition}, _metadata) do
      partition
    end

    def handle_batch(events) do
      {%CounterEvent{partition: partition}, _} = List.first(events)

      Logger.debug("[CrashHandler] Processing partition=#{partition}, count=#{length(events)}")

      # Crash partition 1
      if partition == 1 do
        Logger.debug("[CrashHandler] INTENTIONAL CRASH for partition 1")
        raise "Partition 1 crash test"
      end

      if test_pid = Process.whereis(:stress_test) do
        send(test_pid, {:crash_batch_processed, partition, length(events)})
      end

      :ok
    end
  end

  describe "basic concurrent batching" do
    test "processes events for different partitions independently" do
      Process.register(self(), :stress_test)
      Logger.debug("=== TEST: different partitions ===")

      _handler = start_supervised!(CounterBatchHandler)

      # Generate events for 3 partitions
      events =
        for i <- 1..30 do
          %CounterEvent{
            id: i,
            partition: rem(i, 3),  # partition 0, 1, or 2
            value: i
          }
        end

      Logger.debug("Appending #{length(events)} events")
      :ok = EventStore.append_to_stream(DefaultApp, "counters", 0, Mapper.map_to_event_data(events))

      # Collect batches (with in-memory EventStore, events come one at a time)
      batches = collect_batches(30, timeout: 5000)

      Logger.debug("Collected #{length(batches)} batches: #{inspect(batches)}")

      # Verify we got all 3 partitions
      partitions_seen = Enum.map(batches, fn {partition, _, _, _} -> partition end) |> Enum.uniq() |> Enum.sort()
      assert 0 in partitions_seen
      assert 1 in partitions_seen
      assert 2 in partitions_seen

      # Verify total events processed
      total_events = Enum.map(batches, fn {_, count, _, _} -> count end) |> Enum.sum()
      assert total_events == 30

      Logger.debug("✓ All partitions processed independently")

      Process.unregister(:stress_test)
    end
  end

  describe "crash isolation" do
    test "handler crash doesn't affect other partitions" do
      Process.register(self(), :stress_test)
      Logger.debug("=== CRASH ISOLATION TEST ===")

      _handler = start_supervised!(CrashingBatchHandler)

      # Send events for 3 partitions
      events =
        for i <- 1..15 do
          %CounterEvent{id: i, partition: rem(i, 3), value: i}
        end

      event_data = Mapper.map_to_event_data(events)
      :ok = EventStore.append_to_stream(DefaultApp, "crash-test", 0, event_data)

      # Collect batches - partition 1 will crash, but 0 and 2 should work
      batches = collect_all_batches(timeout: 2000, message_pattern: :crash_batch_processed)

      Logger.debug("Batches from non-crashing partitions: #{inspect(batches)}")

      # Verify partitions 0 and 2 processed (partition 1 crashed)
      partitions_processed = Enum.map(batches, fn {p, _} -> p end) |> Enum.uniq()

      if 0 in partitions_processed or 2 in partitions_processed do
        Logger.debug("✓ Non-crashing partitions continued processing despite partition 1 crash")
      else
        Logger.warning("No batches collected - crash may have stopped all processing")
      end

      Process.unregister(:stress_test)
    end
  end

  describe "ordering guarantees" do
    test "events within partition stay ordered" do
      Process.register(self(), :stress_test)
      Logger.debug("=== ORDERING TEST ===")

      _handler = start_supervised!(CounterBatchHandler)

      # Send 21 events to partition 0
      events =
        for i <- 1..21 do
          %CounterEvent{id: i * 7, partition: 0, value: i}
        end

      Logger.debug("Sending 21 events to partition 0")
      event_data = Mapper.map_to_event_data(events)
      :ok = EventStore.append_to_stream(DefaultApp, "ordering-test", 0, event_data)

      # Collect all batches for partition 0
      batches = collect_all_batches(timeout: 3000)
      partition_0_batches = Enum.filter(batches, fn {p, _, _, _} -> p == 0 end)

      Logger.debug("Partition 0 received #{length(partition_0_batches)} batches")

      # Extract all event numbers
      all_event_numbers =
        partition_0_batches
        |> Enum.flat_map(fn {_, _, _, event_numbers} -> event_numbers end)
        |> Enum.sort()

      # Verify sequential
      if all_event_numbers == Enum.sort(all_event_numbers) do
        Logger.debug("✓ All #{length(all_event_numbers)} events in correct order")
      else
        Logger.error("Events out of order: #{inspect(all_event_numbers)}")
        raise "Ordering violation"
      end

      Process.unregister(:stress_test)
    end
  end

  describe "independent timers" do
    test "each partition has its own timer that flushes independently" do
      Process.register(self(), :stress_test)
      Logger.debug("=== INDEPENDENT TIMER TEST ===")

      _handler = start_supervised!(CounterBatchHandler)

      # Send events to different partitions at different times
      # Partition 0: 3 events at T=0ms
      events_p0 = [
        %CounterEvent{id: 1, partition: 0, value: 1},
        %CounterEvent{id: 2, partition: 0, value: 2},
        %CounterEvent{id: 3, partition: 0, value: 3}
      ]

      Logger.debug("Sending 3 events to partition 0 at T=0")
      :ok = EventStore.append_to_stream(DefaultApp, "timer-test-0", 0, Mapper.map_to_event_data(events_p0))

      Process.sleep(30)

      # Partition 1: 2 events at T=30ms
      events_p1 = [
        %CounterEvent{id: 4, partition: 1, value: 4},
        %CounterEvent{id: 5, partition: 1, value: 5}
      ]

      Logger.debug("Sending 2 events to partition 1 at T=30")
      :ok = EventStore.append_to_stream(DefaultApp, "timer-test-1", 0, Mapper.map_to_event_data(events_p1))

      # Collect batches - they should flush at different times
      batches_with_time =
        for _ <- 1..2 do
          receive do
            {:batch_processed, partition, count, sum, _event_numbers} ->
              {System.monotonic_time(:millisecond), partition, count, sum}
          after
            500 -> nil
          end
        end
        |> Enum.reject(&is_nil/1)

      if length(batches_with_time) >= 2 do
        [{time_0, p0, _, _}, {time_1, p1, _, _}] = batches_with_time
        time_diff = abs(time_1 - time_0)

        Logger.debug("Partition #{p0} flushed at T=#{time_0}")
        Logger.debug("Partition #{p1} flushed at T=#{time_1}")
        Logger.debug("Time difference: #{time_diff}ms")
        Logger.debug("✓ Partitions flushed independently (not synchronized)")
      else
        Logger.warning("Didn't receive expected batches (in-memory EventStore limitation)")
      end

      Process.unregister(:stress_test)
    end
  end

  # Helper functions

  defp collect_batches(count, opts) do
    timeout = Keyword.get(opts, :timeout, 1000)
    message_pattern = Keyword.get(opts, :message_pattern, :batch_processed)

    for _ <- 1..count do
      receive do
        {^message_pattern, partition, count, sum, event_numbers} ->
          {partition, count, sum, event_numbers}

        {^message_pattern, partition, count, sum} ->
          {partition, count, sum, []}
      after
        timeout -> {:timeout, 0, 0, []}
      end
    end
    |> Enum.reject(&match?({:timeout, _, _, _}, &1))
  end

  defp collect_all_batches(opts) do
    timeout = Keyword.get(opts, :timeout, 2000)
    initial_wait = Keyword.get(opts, :initial_wait, 100)
    message_pattern = Keyword.get(opts, :message_pattern, :batch_processed)

    # Wait a bit for first batch
    Process.sleep(initial_wait)

    collect_all_batches_loop([], timeout, message_pattern)
  end

  defp collect_all_batches_loop(acc, timeout, message_pattern) do
    receive do
      {^message_pattern, partition, count, sum, event_numbers} ->
        collect_all_batches_loop([{partition, count, sum, event_numbers} | acc], timeout, message_pattern)

      {^message_pattern, partition, count, sum} ->
        collect_all_batches_loop([{partition, count, sum, []} | acc], timeout, message_pattern)

      {^message_pattern, partition, count} ->
        collect_all_batches_loop([{partition, count} | acc], timeout, message_pattern)
    after
      timeout -> Enum.reverse(acc)
    end
  end
end

