defmodule RegistryTest do
  use ExUnit.Case, asycn: true
  doctest Registry

  setup config do
    kind = config[:kind] || :unique
    partitions = config[:partitions] || 1
    {:ok, _} = Registry.start_link(kind, config.test, partitions: partitions)
    {:ok, %{registry: config.test, partitions: partitions}}
  end

  for {describe, partitions} <- ["with 1 partition": 1, "with 8 partitions": 8] do
    describe "unique #{describe}" do
      @describetag kind: :unique, partitions: partitions

      test "has unique registrations", %{registry: registry} do
        {:ok, pid} = Registry.register(registry, "hello", :value)
        assert is_pid(pid)
        assert Registry.keys(registry, self()) == ["hello"]

        assert {:error, {:already_registered, pid}} =
               Registry.register(registry, "hello", :value)
        assert pid == self()
        assert Registry.keys(registry, self()) == ["hello"]

        {:ok, pid} = Registry.register(registry, "world", :value)
        assert is_pid(pid)
        assert Registry.keys(registry, self()) |> Enum.sort() == ["hello", "world"]
      end

      test "has unique registrations even if partition is delayed", %{registry: registry} do
        {owner, task} = register_task(registry, "hello", :value)
        assert Registry.register(registry, "hello", :other) ==
               {:error, {:already_registered, task}}

        :sys.suspend(owner)
        kill_and_assert_down(task)
        Registry.register(registry, "hello", :other)
        assert Registry.whereis(registry, "hello") == {self(), :other}
      end

      test "finds whereis process considering liveness", %{registry: registry} do
        assert Registry.whereis(registry, "hello") == :error
        {owner, task} = register_task(registry, "hello", :value)
        assert Registry.whereis(registry, "hello") == {task, :value}

        :sys.suspend(owner)
        kill_and_assert_down(task)
        assert Registry.whereis(registry, "hello") == :error
      end

      test "finds whereis linked process considering liveness", %{registry: registry} do
        assert Registry.whereis(registry, "hello") == :error
        {_, task} = register_task(registry, "hello", :value, link: true)
        assert Registry.whereis(registry, "hello") == {task, :value}
        kill_and_assert_down(task)
        assert Registry.whereis(registry, "hello") == :error
      end

      test "returns process keys considering liveness", %{registry: registry} do
        assert Registry.keys(registry, self()) == []
        {owner, task} = register_task(registry, "hello", :value)
        assert Registry.keys(registry, task) == ["hello"]

        :sys.suspend(owner)
        kill_and_assert_down(task)
        assert Registry.keys(registry, task) == []
      end

      test "cleans up tables on process crash", %{registry: registry, partitions: partitions} do
        {_, task1} = register_task(registry, "hello", :value)
        {_, task2} = register_task(registry, "world", :value)

        kill_and_assert_down(task1)
        kill_and_assert_down(task2)

        for i <- 0..partitions-1 do
          [{_, key, {partition, pid}}] = :ets.lookup(registry, i)
          GenServer.call(partition, :sync)
          assert :ets.tab2list(key) == []
          assert :ets.tab2list(pid) == []
        end
      end
    end
  end

  for {describe, partitions} <- ["with 1 partition": 1, "with 8 partitions": 8] do
    describe "duplicate #{describe}" do
      @describetag kind: :duplicate, partitions: partitions

      test "has duplicate registrations", %{registry: registry} do
        {:ok, pid} = Registry.register(registry, "hello", :value)
        assert is_pid(pid)
        assert Registry.keys(registry, self()) == ["hello"]

        assert {:ok, pid} = Registry.register(registry, "hello", :value)
        assert is_pid(pid)
        assert Registry.keys(registry, self()) == ["hello", "hello"]

        {:ok, pid} = Registry.register(registry, "world", :value)
        assert is_pid(pid)
        assert Registry.keys(registry, self()) |> Enum.sort() == ["hello", "hello", "world"]
      end

      test "returns process keys considering liveness", %{registry: registry} do
        assert Registry.keys(registry, self()) == []
        {owner, task} = register_task(registry, "hello", :value)
        assert Registry.keys(registry, task) == ["hello"]

        :sys.suspend(owner)
        kill_and_assert_down(task)
        assert Registry.keys(registry, task) == []
      end

      test "cleans up tables on process crash", %{registry: registry, partitions: partitions} do
        {_, task1} = register_task(registry, "hello", :value)
        {_, task2} = register_task(registry, "world", :value)

        kill_and_assert_down(task1)
        kill_and_assert_down(task2)

        for i <- 0..partitions-1 do
          [{_, key, {partition, pid}}] = :ets.lookup(registry, i)
          GenServer.call(partition, :sync)
          assert :ets.tab2list(key) == []
          assert :ets.tab2list(pid) == []
        end
      end
    end
  end

  defp register_task(registry, key, value, opts \\ []) do
    parent = self()
    {:ok, task} =
      Task.start(fn ->
        {:ok, owner} = Registry.register(registry, key, value)
        opts[:link] && Process.link(owner)
        send(parent, {:ok, owner})
        Process.sleep(:infinity)
      end)
    assert_receive {:ok, owner}
    {owner, task}
  end

  defp kill_and_assert_down(pid) do
    ref = Process.monitor(pid)
    Process.exit(pid, :kill)
    assert_receive {:DOWN, ^ref, _, _, _}
  end
end
