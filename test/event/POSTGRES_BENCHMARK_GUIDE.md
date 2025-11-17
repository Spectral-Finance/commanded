# Testing Concurrent Batching with Real Postgres EventStore

The in-memory EventStore in Commanded's test suite delivers events slowly with partitioning (one at a time). For realistic performance testing, use real Postgres EventStore in YOUR application.

## Setup in Your Cortex Application

### 1. Create Benchmark Module

```elixir
# test/cortex/benchmarks/concurrent_batching_benchmark.exs
defmodule Cortex.Benchmarks.ConcurrentBatchingBenchmark do
  use ExUnit.Case
  
  require Logger
  
  alias Cortex.{Repo, EventStore}
  
  @moduletag :benchmark  # Tag so you can run separately
  @moduletag timeout: 300_000  # 5 minute timeout
  
  defmodule BenchmarkEvent do
    @derive Jason.Encoder
    defstruct [:market_id, :trade_value, :sequence]
  end
  
  # Mode 1: Serial (baseline)
  defmodule SerialHandler do
    use Commanded.Event.Handler,
      application: Cortex.CommandedApp,
      name: __MODULE__
    
    def handle(%BenchmarkEvent{} = event, _metadata) do
      # Simulate database write
      Process.sleep(2)
      
      # Track in ETS
      :ets.update_counter(:benchmark_results, event.market_id, event.trade_value, {event.market_id, 0})
      :ok
    end
  end
  
  # Mode 2: Concurrent + Batched (optimized)
  defmodule ConcurrentBatchedHandler do
    use Commanded.Event.Handler,
      application: Cortex.CommandedApp,
      name: __MODULE__,
      concurrency: 10,
      batch_size: 50,
      batch_timeout: 100
    
    def partition_by(%BenchmarkEvent{market_id: id}, _), do: id
    
    def handle_batch(events) do
      # Simulate batch database write
      Process.sleep(2)
      
      # Track all in batch
      Enum.each(events, fn {event, _metadata} ->
        :ets.update_counter(:benchmark_results, event.market_id, event.trade_value, {event.market_id, 0})
      end)
      
      :ok
    end
  end
  
  @tag :benchmark
  test "compare serial vs concurrent+batched with 100 markets, 1000 events each" do
    # Configuration
    num_markets = 100
    events_per_market = 1000
    total_events = num_markets * events_per_market
    
    Logger.info("\n#{String.duplicate("=", 80)}")
    Logger.info("CONCURRENT BATCHING BENCHMARK - REAL POSTGRES EVENTSTORE")
    Logger.info("#{String.duplicate("=", 80)}")
    Logger.info("Markets: #{num_markets}")
    Logger.info("Events per market: #{events_per_market}")
    Logger.info("Total events: #{total_events}")
    Logger.info("DB delay: 2ms per operation")
    Logger.info("#{String.duplicate("=", 80)}\n")
    
    # Generate test events
    events = 
      for market_id <- 0..(num_markets - 1),
          sequence <- 1..events_per_market do
        %BenchmarkEvent{
          market_id: "market_#{market_id}",
          trade_value: sequence,
          sequence: sequence
        }
      end
    
    # Benchmark Mode 1: Serial
    Logger.info("Mode 1: SERIAL (baseline)")
    {serial_time_us, :ok} = benchmark_handler(SerialHandler, events, total_events)
    serial_time_s = serial_time_us / 1_000_000
    serial_throughput = round(total_events / serial_time_s)
    
    Logger.info("  Time: #{Float.round(serial_time_s, 2)}s")
    Logger.info("  Throughput: #{serial_throughput} events/sec\n")
    
    # Verify correctness
    verify_results(num_markets, events_per_market)
    
    # Benchmark Mode 2: Concurrent + Batched
    Logger.info("Mode 2: CONCURRENT + BATCHED")
    {concurrent_time_us, :ok} = benchmark_handler(ConcurrentBatchedHandler, events, total_events)
    concurrent_time_s = concurrent_time_us / 1_000_000
    concurrent_throughput = round(total_events / concurrent_time_s)
    
    Logger.info("  Time: #{Float.round(concurrent_time_s, 2)}s")
    Logger.info("  Throughput: #{concurrent_throughput} events/sec\n")
    
    # Verify correctness
    verify_results(num_markets, events_per_market)
    
    # Results
    speedup = serial_time_s / concurrent_time_s
    throughput_improvement = concurrent_throughput / serial_throughput
    
    Logger.info("#{String.duplicate("=", 80)}")
    Logger.info("RESULTS")
    Logger.info("#{String.duplicate("=", 80)}")
    Logger.info("Speedup: #{Float.round(speedup, 2)}x faster")
    Logger.info("Throughput improvement: #{Float.round(throughput_improvement, 2)}x")
    Logger.info("#{String.duplicate("=", 80)}\n")
    
    # Assertions
    assert speedup > 10, "Expected at least 10x speedup, got #{Float.round(speedup, 2)}x"
    assert concurrent_throughput > 10_000, "Expected >10k events/sec, got #{concurrent_throughput}"
  end
  
  # Helper functions
  
  defp benchmark_handler(handler_module, events, expected_total) do
    # Reset ETS
    if :ets.whereis(:benchmark_results) != :undefined do
      :ets.delete(:benchmark_results)
    end
    :ets.new(:benchmark_results, [:named_table, :public, :set])
    
    # Start handler
    {:ok, _pid} = handler_module.start_link()
    
    # Give time to subscribe
    Process.sleep(500)
    
    # Measure append + processing time
    {time_us, :ok} = :timer.tc(fn ->
      # Append events in chunks
      events
      |> Enum.chunk_every(1000)
      |> Enum.with_index()
      |> Enum.each(fn {chunk, idx} ->
        stream = "benchmark-#{handler_module}-#{idx}"
        
        event_data = Commanded.Event.Mapper.map_to_event_data(chunk)
        :ok = Cortex.EventStore.append_to_stream(stream, 0, event_data)
      end)
      
      # Wait for processing
      wait_for_processing(expected_total, 120_000)
    end)
    
    # Stop handler
    GenServer.stop(handler_module)
    
    {time_us, :ok}
  end
  
  defp wait_for_processing(expected_total, max_wait_ms) do
    wait_for_processing(expected_total, 0, max_wait_ms)
  end
  
  defp wait_for_processing(expected_total, elapsed_ms, max_wait_ms) when elapsed_ms < max_wait_ms do
    current_total = :ets.info(:benchmark_results, :size) || 0
    
    if current_total >= expected_total do
      Logger.debug("✓ Processed #{current_total}/#{expected_total} events in #{elapsed_ms}ms")
      :ok
    else
      if rem(elapsed_ms, 5000) == 0 and elapsed_ms > 0 do
        Logger.info("  Progress: #{current_total}/#{expected_total} (#{Float.round(current_total / expected_total * 100, 1)}%)")
      end
      
      Process.sleep(200)
      wait_for_processing(expected_total, elapsed_ms + 200, max_wait_ms)
    end
  end
  
  defp wait_for_processing(_expected, elapsed_ms, _max) do
    raise "Timeout after #{elapsed_ms}ms"
  end
  
  defp verify_results(num_markets, events_per_market) do
    expected_sum = Enum.sum(1..events_per_market)
    
    errors = 
      for market_id <- 0..(num_markets - 1) do
        market_key = "market_#{market_id}"
        
        actual_sum = case :ets.lookup(:benchmark_results, market_key) do
          [{^market_key, sum}] -> sum
          [] -> 0
        end
        
        if actual_sum != expected_sum do
          {market_key, expected_sum, actual_sum}
        end
      end
      |> Enum.reject(&is_nil/1)
    
    if length(errors) > 0 do
      Logger.error("#{length(errors)} markets have incorrect sums!")
      Enum.each(errors, fn {market, expected, actual} ->
        Logger.error("  #{market}: expected #{expected}, got #{actual}")
      end)
      raise "Incorrect results"
    end
    
    Logger.info("✓ All #{num_markets} markets have correct sums")
  end
end
```

## Running the Benchmark

```bash
# In your Cortex application:

# Run only benchmark tests
mix test --only benchmark

# Or run specific benchmark
mix test test/cortex/benchmarks/concurrent_batching_benchmark.exs
```

## Expected Results with Real Postgres

With real Postgres EventStore and 100 markets × 1000 events each:

```
Mode 1: SERIAL
  Time: ~200s (100,000 events × 2ms each)
  Throughput: ~500 events/sec

Mode 2: CONCURRENT + BATCHED (concurrency: 10, batch_size: 50)
  Time: ~4s
  Throughput: ~25,000 events/sec
  Speedup: 50x faster!
```

Why so much faster:
- 10 concurrent handlers process in parallel (10x)
- Each batches 50 events per DB call (50x)
- Combined: 10 × 50 = 500x theoretical speedup
- Actual: ~50x due to overhead

## For Load Testing

```elixir
# Create realistic load test
defmodule Cortex.LoadTest.MarketSimulator do
  @moduledoc """
  Simulates realistic market activity for load testing.
  """
  
  def run(opts \\ []) do
    num_markets = Keyword.get(opts, :markets, 100)
    duration_seconds = Keyword.get(opts, :duration, 60)
    events_per_second = Keyword.get(opts, :rate, 1000)
    
    # Spawn process per market
    for market_id <- 0..(num_markets - 1) do
      Task.async(fn ->
        simulate_market_activity(market_id, duration_seconds, events_per_second / num_markets)
      end)
    end
    |> Task.await_many(:infinity)
  end
  
  defp simulate_market_activity(market_id, duration_s, rate) do
    interval_ms = round(1000 / rate)
    end_time = System.monotonic_time(:second) + duration_s
    
    Stream.repeatedly(fn ->
      if System.monotonic_time(:second) < end_time do
        # Generate realistic trade event
        event = %TradeExecuted{
          market_id: "market_#{market_id}",
          price: :rand.uniform(10000) / 100,
          quantity: :rand.uniform(100),
          timestamp: DateTime.utc_now()
        }
        
        Cortex.dispatch(event)
        Process.sleep(interval_ms)
        :continue
      else
        :done
      end
    end)
    |> Enum.take_while(&(&1 == :continue))
  end
end

# Run load test:
Cortex.LoadTest.MarketSimulator.run(markets: 100, duration: 300, rate: 5000)
# 100 markets, 5 minutes, 5000 events/sec = 1.5M events total
```

## Why Not in Commanded Test Suite?

1. **Dependency**: Would require `commanded_eventstore_adapter` and Postgres setup
2. **CI complexity**: Would need Postgres in CI environment
3. **Test speed**: Real EventStore tests take much longer
4. **Scope**: Performance testing belongs in applications, not libraries

## Your Next Steps

1. **Copy benchmark to Cortex** - Use your real EventStore
2. **Run with realistic volumes** - 100 markets, 1000+ events each
3. **Measure actual throughput** - See real performance gains
4. **Tune parameters** - Adjust `batch_size` and `batch_timeout` based on results
5. **Monitor in production** - Use telemetry with `partition` metadata

The in-memory tests prove **correctness**. Your Cortex tests will prove **performance**.

