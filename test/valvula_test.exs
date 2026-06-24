defmodule ValvulaTest do
  @moduledoc """
  End-to-end tests for the `Valvula` facade + `Valvula.Server` + `Valvula.Bucket`.

  Each test uses a unique server name to allow `async: true`.
  """
  use ExUnit.Case, async: true

  alias Valvula.Bucket

  setup do
    name = :"test_limiter_#{System.unique_integer([:positive])}"
    start_supervised!({Valvula, name: name, rate: 5, window: 100, burst: 0})
    %{server: name}
  end

  describe "consume/2 — basic flow" do
    test "allows requests within the rate", %{server: server} do
      for _ <- 1..5 do
        assert :ok = Valvula.consume(server, key: "user")
      end
    end

    test "rejects requests over the rate", %{server: server} do
      for _ <- 1..5, do: Valvula.consume(server, key: "user")
      assert {:error, :rate_limited, retry_ms} = Valvula.consume(server, key: "user")
      assert is_integer(retry_ms)
      assert retry_ms >= 0
    end

    test "different keys are independent", %{server: server} do
      for _ <- 1..5, do: Valvula.consume(server, key: "user_a")
      assert {:error, :rate_limited, _} = Valvula.consume(server, key: "user_a")
      # user_b still has a full bucket
      assert :ok = Valvula.consume(server, key: "user_b")
    end

    test "tokens refill after window elapses", %{server: server} do
      for _ <- 1..5, do: Valvula.consume(server, key: "refill_user")
      assert {:error, :rate_limited, _} = Valvula.consume(server, key: "refill_user")

      Process.sleep(110)  # wait > window (100ms)
      assert :ok = Valvula.consume(server, key: "refill_user")
    end

    test "tokens: option consumes N at once", %{server: server} do
      assert :ok = Valvula.consume(server, key: "bulk", tokens: 3)
      assert :ok = Valvula.consume(server, key: "bulk", tokens: 2)
      assert {:error, :rate_limited, _} = Valvula.consume(server, key: "bulk")
    end

    test "tokens: 0 raises ArgumentError", %{server: server} do
      assert_raise ArgumentError, fn ->
        Valvula.consume(server, key: "x", tokens: 0)
      end
    end
  end

  describe "reset/2" do
    test "reset restores a bucket to full", %{server: server} do
      for _ <- 1..5, do: Valvula.consume(server, key: "reset_user")
      :ok = Valvula.reset(server, key: "reset_user")
      assert :ok = Valvula.consume(server, key: "reset_user")
    end

    test "reset on a never-seen key creates a fresh bucket", %{server: server} do
      assert :ok = Valvula.reset(server, key: "ghost")
      assert :ok = Valvula.consume(server, key: "ghost")
    end
  end

  describe "status/2" do
    test "returns the current bucket state", %{server: server} do
      Valvula.consume(server, key: "status_user")
      status = Valvula.status(server, key: "status_user")

      assert status.tokens == 4
      assert status.max == 5
      assert status.limited == false
      assert status.window_ms == 100
      assert status.rate == 5
      assert status.burst == 0
      assert status.consumed_total == 1
      assert status.rejected_total == 0
    end

    test "limited: true when bucket is empty", %{server: server} do
      for _ <- 1..5, do: Valvula.consume(server, key: "empty_user")
      status = Valvula.status(server, key: "empty_user")
      assert status.tokens == 0
      assert status.limited == true
    end

    test "synthetic full bucket for unseen key", %{server: server} do
      status = Valvula.status(server, key: "never_seen")
      assert status.tokens == 5
      assert status.limited == false
      assert status.consumed_total == 0
    end
  end

  describe "stats/1" do
    test "returns aggregate counters", %{server: server} do
      Valvula.consume(server, key: "a")
      Valvula.consume(server, key: "b", tokens: 2)
      # Drain key "a" then trigger a rejection:
      for _ <- 1..4, do: Valvula.consume(server, key: "a")
      _rejected = Valvula.consume(server, key: "a")

      stats = Valvula.stats(server)

      assert stats.rate == 5
      assert stats.window_ms == 100
      assert stats.burst == 0
      assert stats.consumed_total == 7   # 1 + 2 + 4 = 7 consumes succeeded
      assert stats.rejected_total == 1
      assert stats.bucket_count >= 2
    end
  end

  describe "lookup/2" do
    test "returns {:ok, bucket} for known key", %{server: server} do
      Valvula.consume(server, key: "lookup_user")
      assert {:ok, %Valvula.Bucket{}} = Valvula.lookup(server, "lookup_user")
    end

    test "returns :error for unknown key", %{server: server} do
      assert :error = Valvula.lookup(server, "ghost")
    end
  end

  describe "configuration validation" do
    test "raises on rate: 0" do
      assert_raise ArgumentError, fn ->
        Valvula.start_link(name: :"bad_rate_#{System.unique_integer()}", rate: 0, window: 100)
      end
    end

    test "raises on missing :name" do
      assert_raise KeyError, fn ->
        Valvula.start_link(rate: 5, window: 100)
      end
    end

    test "accepts integer window in ms", %{server: _} do
      name = :"int_window_#{System.unique_integer()}"
      assert {:ok, _} = Valvula.start_link(name: name, rate: 5, window: 1_000)
      assert :ok = Valvula.consume(name, key: "x")
      GenServer.stop(name)
    end
  end

  describe "Valvula.Bucket (unit)" do
    test "new/2 creates a full bucket" do
      b = Bucket.new("k", %{max_tokens: 10, refill_rate: 5, window_ms: 1000})
      assert b.tokens == 10
      assert b.max_tokens == 10
      assert b.consumed_total == 0
    end

    test "refill/2 adds tokens lazily" do
      b = Bucket.new("k", %{max_tokens: 10, refill_rate: 5, window_ms: 1000})
      # Pretend 2000ms have passed
      now = b.last_refill + 2000
      refilled = Bucket.refill(b, now)
      # 2 windows × 5 tokens = 10 → capped at max_tokens
      assert refilled.tokens == 10
    end

    test "refill/2 caps at max_tokens" do
      b = Bucket.new("k", %{max_tokens: 5, refill_rate: 100, window_ms: 1000})
      now = b.last_refill + 60_000
      refilled = Bucket.refill(b, now)
      assert refilled.tokens == 5
    end

    test "retry_after_ms is 0 when enough tokens" do
      b = Bucket.new("k", %{max_tokens: 10, refill_rate: 5, window_ms: 1000})
      assert Bucket.retry_after_ms(b, 5, b.last_refill) == 0
    end

    test "retry_after_ms is non-negative when not enough" do
      b = %{Bucket.new("k", %{max_tokens: 10, refill_rate: 5, window_ms: 1000}) | tokens: 0}
      # Need 10 tokens, refills 5 per second → need 2 windows = 2000ms
      retry = Bucket.retry_after_ms(b, 10, b.last_refill)
      assert is_integer(retry)
      assert retry >= 1000
    end
  end
end
