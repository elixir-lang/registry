defmodule Registry.Sojourn do
  @moduledoc """
  A registry-based sojourn pool.

  When started, it requires the number of processes being pooled:

      Registry.Sojourn.start_link(MyApp.Pool, 4)

  Now processes can be started and registered under the pool registry
  as usual. The only restriction is that the key has the format of
  `{term, id}` where id is an integer between `0..pool_size-1`:

      GenServer.start_link(module, arg, name: {:via, Registry, {MyApp.Pool, {:example, 0}}})
      GenServer.start_link(module, arg, name: {:via, Registry, {MyApp.Pool, {:example, 1}}})
      GenServer.start_link(module, arg, name: {:via, Registry, {MyApp.Pool, {:example, 2}}})
      GenServer.start_link(module, arg, name: {:via, Registry, {MyApp.Pool, {:example, 3}}})

  In the example above, all processes are under the pool key `:example`.
  All sojourn pooled processes will receive periodic messages from the
  pool and it must handle them as below:

      def handle_info({:sample, module, arg}, state) do
        module.sample(arg)
        {:noreply, state}
      end

  Now pool lookups can be done by using a `Registry.Sojourn` `:via` tuple:

      name = {:via, Registry.Sojourn, {MyApp.Pool, :example}}
      GenServer.call(name, :hello)

  And that's it. To sum up:

    1. Start the pool
    2. Register processes using the `Registry` :via tuple
    3. Add the `handle_info/2` callback clause
    4. Lookup processes using the `Registry.Sojourn` :via tuple

  """

  require Logger

  @opaque sample :: {Registry.registry, Registry.key, non_neg_integer}

  @doc """
  Starts the sojourn pool supervisor.

  It expects the pool `name`, the `pool_size` and a set of options.

  ## Options

    * `:interval` - the Sojourn sampling interval
    * `:partitions` - how many partitions in the registry

  """
  def start_link(name, pool_size, options \\ [])
      when is_atom(name) and is_integer(pool_size) and pool_size > 0 do
    import Supervisor.Spec

    sojourn  = Module.concat(name, "Sojourn")
    worker   = [name: sojourn]
    registry = [listeners: [sojourn], info: [sojourn: pool_size],
                partitions: Keyword.get(options, :partitions, 1)]

    children = [
      worker(GenServer, [__MODULE__, {name, pool_size, options}, worker]),
      supervisor(Registry, [:unique, name, registry])
    ]

    Supervisor.start_link(children, strategy: :one_for_all)
  end

  @doc """
  Sample callback invoked by the registered process.
  """
  @spec sample(sample) :: :ok | :error
  def sample({name, key, send_time}) do
    receive_time = System.system_time

    update =
      Registry.update(name, key, fn _ ->
        {-send_time, receive_time - send_time}
      end)

    case update do
      {_, _} -> :ok
      :error -> :error
    end
  end

  defp normalize(nil), do: {0, 0}
  defp normalize(other), do: other

  # Via callbacks

  @doc false
  def whereis_name({registry, key}) do
    {:ok, size} = Registry.info(registry, :sojourn)
    one = :rand.uniform(size) - 1
    two = :rand.uniform(size) - 1

    case {Registry.lookup(registry, {key, one}), Registry.lookup(registry, {key, two})} do
      {[], []} -> :undefined
      {[], [{pid, _}]} -> pid
      {[{pid, _}], []} -> pid
      {[{p1, v1}], [{p2, v2}]} ->
        if normalize(v1) < normalize(v2), do: p1, else: p2
    end
  end

  @doc false
  def send(key, msg) do
    case whereis_name(key) do
      :undefined -> :erlang.error(:badarg, [key, msg])
      pid -> Kernel.send(pid, msg)
    end
  end

  # GenServer Callbacks

  use GenServer

  def init({name, pool_size, options}) do
    interval = Keyword.get(options, :interval, 100)
    send_sample(interval)
    {:ok, %{name: name, pool_size: pool_size, pids: []}}
  end

  def handle_info({:subscribe, _, key, pid, _value}, %{pool_size: pool_size} = state) do
    case key do
      {_, id} when id in 0..pool_size-1 ->
        :ok
      _ ->
        Logger.warn "Registry.Sojourn expects registry key to be {term, id} " <>
                    "where id is between 0..#{pool_size-1}, got: #{inspect key}"
    end
    {:noreply, update_in(state.pids, &[{pid, key} | &1])}
  end
  def handle_info({:unsubscribe, _, key, pid}, state) do
    {:noreply, update_in(state.pids, &List.delete(&1, {pid, key}))}
  end
  def handle_info({:sample, interval}, %{pids: pids, name: name} = state) do
    time = System.system_time

    for {pid, key} <- pids do
      Kernel.send(pid, {:sample, __MODULE__, {name, key, time}})
    end

    send_sample(interval)
    {:noreply, state}
  end

  defp send_sample(interval) do
    Process.send_after(self(), {:sample, interval}, interval)
  end
end