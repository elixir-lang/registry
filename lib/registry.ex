defmodule Registry do
  @moduledoc """
  A local and scalable key-value process storage.

  It allows developers to lookup one or more process with a given key.
  If the registry has `:unique` keys, a key points to 0 or 1 processes.
  If the registry allows `:duplicate` keys, a single key may point to 0,
  1 or many processes. In both cases, a process may have multiple keys.

  Each entry in the registry is associated to the process that has
  registered the key. If the process crashes, the keys associated to that
  process are automatically removed, albeit with a possible delay.

  The registry can be used for name lookups, using the `:via` option,
  for storing properties,  for custom dispatching rules as well as a
  pubsub implementation. We explore those scenarios below.

  The registry may also be transparently partitioned, which provides
  more scalable behaviour for running registries on highly concurrent
  environments with thousands or millions of entries.

  Looking up, dispatching and registering is efficient at the cost of
  delayed unsubscription. For example, if a process crashes, its keys
  are automatically removed from the registry but the change may not
  propagate immediately. This means that, if you are looking up by key,
  the key may point to a process that is already dead. However, this is
  typically not an issue. After all, a process referenced by a pid may
  crash at any time, including between getting the value from the registry
  and sending it a message. Many parts of the standard library are designed
  to cope with that, such as `Process.monitor/1` which will deliver the
  DOWN message immediately if the monitored process is already dead and
  `Kernel.send/2` which acts as a no-op for dead processes.
  """

  @kind [:unique, :duplicate]

  @type name :: atom
  @type kind :: :unique | :duplicate
  @type key :: term
  @type value :: term

  # TODO: Getting a list of all keys for a given process needs
  # to consider :unique or :duplicate and call Enum.uniq accordingly.

  # The registry is a supervised process.
  # The registry uses one ets plus one ets table per partition.
  # Options can be any keyword list which are stored in the registry.
  # The only key with meaning specific to the Registry is :partitions.

  @spec start_link(kind, name, Keyword.t) :: {:ok, pid} | {:error, term}
  def start_link(kind, name, options \\ []) when kind in @kind and is_atom(name) do
    unless Keyword.keyword?(options) do
      raise ArgumentError, "expected Registry options to be a keyword list, got: #{inspect options}"
    end
    options = Keyword.put_new(options, :partitions, 1)
    Registry.Supervisor.start_link(kind, name, options)
  end

  @doc """
  Finds the process for the given `key` in the unique `name` registry.

  Returns `{pid, value}` if the registered `pid` is alive where
  `value` is the term associated to the pid on registration. Otherwise
  it returns `nil`.

  `ArgumentError` will be raised if a non-unique registry name is given.

  ## Examples

  In the example below we register the current process and look it up
  both from itself and other processes.

      iex> Registry.start_link(:unique, Registry.WhereIsTest)
      iex> {:ok, _} = Registry.register(Registry.WhereIsTest, "hello", :world)
      iex> Registry.whereis(Registry.WhereIsTest, "hello")
      {self(), :world}
      iex> Task.async(fn -> Registry.whereis(Registry.WhereIsTest, "hello") end) |> Task.await
      {self(), :world}
      iex> Registry.whereis(Registry.WhereIsTest, "unknown")
      nil

  """
  @spec whereis(name, key) :: {pid, value} | :error
  def whereis(name, key) when is_atom(name) do
    case info!(name) do
      {:unique, partitions} ->
        key_partition = :erlang.phash2(key, partitions)
        key_ets = key_ets!(name, key_partition)
        case :ets.lookup(key_ets, key) do
          [{^key, {pid, _} = pair}] ->
            if Process.alive?(pid), do: pair, else: nil
          [] ->
            nil
        end
      {kind, _} ->
        raise ArgumentError, "Registry.whereis/2 not supported for #{kind} registries"
    end
  end

  @doc """
  Returns the known keys for the given `pid` in `name`.

  If the registry is unique, the keys are unique. Otherwise
  they may contain duplicates if the process was registered
  under the same key multiple times. The list will be empty
  if the process is dead or it has no keys in this registry.

  ## Examples

  Registering under a unique registry does not allow multiple entries:

      iex> Registry.start_link(:unique, Registry.UniqueKeysTest)
      iex> {:ok, _} = Registry.register(Registry.UniqueKeysTest, "hello", :world)
      iex> Registry.register(Registry.UniqueKeysTest, "hello", :later)
      {:error, {:already_registered, self()}}
      iex> Registry.keys(Registry.UniqueKeysTest, self())
      ["hello"]

  Such is possible for duplicate registries though:

      iex> Registry.start_link(:duplicate, Registry.DuplicateKeysTest)
      iex> {:ok, _} = Registry.register(Registry.DuplicateKeysTest, "hello", :world)
      iex> {:ok, _} = Registry.register(Registry.DuplicateKeysTest, "hello", :world)
      iex> Registry.keys(Registry.DuplicateKeysTest, self())
      ["hello", "hello"]
  """
  @spec keys(name, pid) :: [key]
  def keys(name, pid) when is_atom(name) and is_pid(pid) do
    {kind, partitions} = info!(name)
    pid_partition = :erlang.phash2(pid, partitions)
    {_, pid_ets} = pid_ets!(name, pid_partition)
    keys = :ets.lookup_element(pid_ets, pid, 2)
    cond do
      not Process.alive?(pid) -> []
      kind == :unique -> Enum.uniq(keys)
      true -> keys
    end
  end

  @doc """
  Registers the current process under the given `key` in registry.

  A value to be associated with this registration must also be given.
  This value will be retrieved whenever dispatching or doing a key
  lookup.

  This function returns `{:ok, owner}` or `{:error, reason}`.
  The `owner` is the pid of the registry partition responsible for
  the pid and may be linked or monitored by the caller to react to
  failures.

  If the registry has unique keys, it will return `:ok` unless the
  key is already associated to a pid, in which case it returns
  `{:error, {:already_registered, pid}}`.

  If the registry has duplicate keys, multiple registrations from the
  current process under the same key are allowed.

  ## Examples

  Registering under a unique registry does not allow multiple entries:

      iex> Registry.start_link(:unique, Registry.UniqueRegisterTest)
      iex> {:ok, _} = Registry.register(Registry.UniqueRegisterTest, "hello", :world)
      iex> Registry.register(Registry.UniqueRegisterTest, "hello", :later)
      {:error, {:already_registered, self()}}
      iex> Registry.keys(Registry.UniqueRegisterTest, self())
      ["hello"]

  Such is possible for duplicate registries though:

      iex> Registry.start_link(:duplicate, Registry.DuplicateRegisterTest)
      iex> {:ok, _} = Registry.register(Registry.DuplicateRegisterTest, "hello", :world)
      iex> {:ok, _} = Registry.register(Registry.DuplicateRegisterTest, "hello", :world)
      iex> Registry.keys(Registry.DuplicateRegisterTest, self())
      ["hello", "hello"]

  """
  @spec register(name, key, value) :: {:ok, pid} | {:error, {:already_registered, pid}}
  def register(name, key, value) when is_atom(name) do
    self = self()
    {kind, partitions} = info!(name)
    {key_partition, pid_partition} =
      Registry.Partition.partitions(kind, key, self, partitions)
    key_ets = key_ets!(name, key_partition)
    {pid_server, pid_ets} = pid_ets!(name, pid_partition)

    :ok = GenServer.cast(pid_server, {:monitor, self})
    # Register first in the pid ets table because it will always
    # be able to do the clean up. If we register first to the key
    # one and the process crashes, the key will stay there forever.
    true = :ets.insert(pid_ets, {self, key, key_partition})
    register_key(kind, pid_server, key_ets, key, {key, {self, value}})
  end

  defp register_key(:duplicate, pid_server, key_ets, _key, entry) do
    true = :ets.insert(key_ets, entry)
    {:ok, pid_server}
  end
  defp register_key(:unique, pid_server, key_ets, key, entry) do
    if :ets.insert_new(key_ets, entry) do
      {:ok, pid_server}
    else
      # Notice we have to call register_key recursively
      # because we are always at odds of a race condition.
      case :ets.lookup(key_ets, key) do
        [{^key, {pid, _}} = current] ->
          if Process.alive?(pid) do
            {:error, {:already_registered, pid}}
          else
            :ets.delete_object(key_ets, current)
            register_key(:unique, pid_server, key_ets, key, entry)
          end
        [] ->
          register_key(:unique, pid_server, key_ets, key, entry)
      end
    end
  end

  @doc """
  Reads registry options given on `start_link/3`.

  ## Examples

      iex> Registry.start_link(:unique, Registry.InfoDocTest, custom_key: "custom_value")
      iex> Registry.info(Registry.InfoDocTest, :custom_key)
      {:ok, "custom_value"}
      iex> Registry.info(Registry.InfoDocTest, :unknown_key)
      :error

  """
  @spec info(name, info_key :: atom) :: {:ok, info_value :: term} | :error
  def info(name, key) when is_atom(name) and is_atom(key) do
    try do
      :ets.lookup(name, key)
    catch
      :error, :badarg ->
        raise ArgumentError, "unknown registry: #{inspect name}"
    else
      [{^key, value}] -> {:ok, value}
      _ -> :error
    end
  end

  ## Helpers

  defp info!(name) do
    try do
      :ets.lookup(name, :__info__)
    catch
      :error, :badarg ->
        raise ArgumentError, "unknown registry: #{inspect name}"
    else
      [{:__info__, kind, partitions}] -> {kind, partitions}
    end
  end

  defp key_ets!(name, partition) do
    :ets.lookup_element(name, partition, 2)
  end

  defp pid_ets!(name, partition) do
    :ets.lookup_element(name, partition, 3)
  end
end

defmodule Registry.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(kind, name, options) do
    Supervisor.start_link(__MODULE__, {kind, name, options}, name: name)
  end

  def init({kind, name, options}) do
    partitions = Keyword.get(options, :partitions, 1)
    ^name = :ets.new(name, [:set, :public, :named_table, read_concurrency: true])
    true = :ets.insert(name, options)
    true = :ets.insert(name, {:__info__, kind, partitions})

    names =
      for i <- 0..partitions-1 do
        {i, Registry.Partition.key_name(name, i), Registry.Partition.pid_name(name, i)}
      end

    keys = for {i, key_partition, _} <- names, do: {i, key_partition}, into: %{}

    children =
      for {i, key_partition, pid_partition} <- names do
        arg = {kind, name, i, key_partition, pid_partition, keys}
        worker(Registry.Partition, [pid_partition, arg], id: pid_partition)
      end

    supervise(children, strategy: strategy_for_kind(kind))
  end

  # Unique registries have their key partition hashed by key.
  # This means that, if a pid partition crashes, it may have
  # entries from all key partitions, so we need to crash all.
  defp strategy_for_kind(:unique), do: :one_for_all

  # Duplicate registries have both key and pid partitions hashed
  # by pid. This means that, if a pid partition crashes, all of
  # its associated entries are in its sibling table, so we crash one.
  defp strategy_for_kind(:duplicate), do: :one_for_one
end

defmodule Registry.Partition do
  @moduledoc false

  # This process owns the equivalent key and pid ets tables
  # and is responsible for monitoring processes that map to
  # the its own pid table.
  use GenServer

  @doc """
  Returns the name of key partition table.
  """
  @spec key_name(atom, non_neg_integer) :: atom
  def key_name(name, partition) do
    Module.concat([name, "KeyPartition" <> Integer.to_string(partition)])
  end

  @doc """
  Returns the name of pid partition table.
  """
  @spec pid_name(atom, non_neg_integer) :: atom
  def pid_name(name, partition) do
    Module.concat([name, "PIDPartition" <> Integer.to_string(partition)])
  end

  @doc """
  Receives the kind, key, pid and number of partitions and
  returns the value for the key partition.
  """
  @spec key_partition(Registry.kind, term, pid, non_neg_integer) :: non_neg_integer
  def key_partition(:unique, key, _pid, partitions) do
    :erlang.phash2(key, partitions)
  end
  def key_partition(:duplicate, _key, pid, partitions) do
    :erlang.phash2(pid, partitions)
  end

  @doc """
  Receives the pid and number of partitions and returns the
  value for the pid partition.
  """
  @spec pid_partition(pid, non_neg_integer) :: non_neg_integer
  def pid_partition(pid, partitions) do
    :erlang.phash2(pid, partitions)
  end

  @doc """
  Receives the kind, key, pid and number of partitions and
  returns the value for both key and pid partitions.
  """
  @spec partitions(Registry.kind, term, pid, non_neg_integer) :: {non_neg_integer, non_neg_integer}
  def partitions(:unique, key, pid, partitions) do
    {:erlang.phash2(key, partitions), :erlang.phash2(pid, partitions)}
  end
  def partitions(:duplicate, _key, pid, partitions) do
    partition = :erlang.phash2(pid, partitions)
    {partition, partition}
  end

  @doc """
  Starts the registry partition.

  The process is only responsible for monitoring, demonitoring
  and cleaning up when monitored processes crash.
  """
  def start_link(name, arg) do
    GenServer.start_link(__MODULE__, arg, name: name)
  end

  ## Callbacks

  def init({kind, name, i, key_partition, pid_partition, keys}) do
    Process.flag(:trap_exit, true)
    key_ets = init_key_ets(kind, key_partition)
    pid_ets = init_pid_ets(kind, pid_partition)
    true = :ets.insert(name, {i, key_ets, {self(), pid_ets}})
    {:ok, %{pid_partition: pid_partition, keys: keys, monitors: %{}}}
  end

  # The key partition is a set for unique keys,
  # duplicate bag for duplicate ones.
  defp init_key_ets(:unique, key_partition) do
    :ets.new(key_partition, [:set, :public, read_concurrency: true, write_concurrency: true])
  end
  defp init_key_ets(:duplicate, key_partition) do
    :ets.new(key_partition, [:duplicate_bag, :public, read_concurrency: true, write_concurrency: true])
  end

  # A process can always have multiple keys, so the
  # pid partition is always a duplicate bag.
  defp init_pid_ets(_, pid_partition) do
    :ets.new(pid_partition, [:duplicate_bag, :public, read_concurrency: true, write_concurrency: true])
  end

  def handle_cast({:monitor, pid}, state) do
    {:noreply, put_new_monitor(state, pid)}
  end
  def handle_cast({:demonitor, pid}, state) do
    {:noreply, delete_monitor(state, pid)}
  end

  def handle_info({:DOWN, _ref, _type, pid, _info}, state) do
    %{pid_partition: pid_partition, keys: keys} = state

    try do
      entries = :ets.take(pid_partition, pid)
      for {_pid, key, key_partition} <- entries do
        try do
          key_ets = Map.fetch!(keys, key_partition)
          true = :ets.match_delete(key_ets, {key, {pid, :_}})
        catch
          :error, :badarg -> :badarg
        end
      end
    catch
      :error, :badarg -> :badarg
    end

    {:noreply, delete_monitor(state, pid)}
  end
  def handle_info(msg, state) do
    super(msg, state)
  end

  defp put_new_monitor(%{monitors: monitors} = state, pid) do
    case monitors do
      %{^pid => _} -> state
      %{} -> %{state | monitors: Map.put(monitors, pid, Process.monitor(pid))}
    end
  end

  defp delete_monitor(%{monitors: monitors} = state, pid) do
    case monitors do
      %{^pid => ref} ->
        Process.demonitor(ref)
        %{state | monitors: Map.delete(monitors, pid)}
      %{} ->
        state
    end
  end
end
