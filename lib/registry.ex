defmodule Registry do
  @moduledoc """
  A local and scalable key-value process storage.

  It allows developers to lookup one or more process with a given key.
  If the registry has `:unique` keys, a key points to 0 or 1 processes.
  If the registry allows `:duplicate` keys, a single key may point to 0,
  1 or many processes. In both cases, a process may have multiple keys.

  Each entry in the registry is associated to the process that has
  registered the key. If the process crashes, the keys associated to that
  process are automatically removed.

  The registry can be used for name lookups, using the `:via` option,
  for storing properties, for custom dispatching rules as well as a
  pubsub implementation. We explore those scenarios below.

  The registry may also be transparently partitioned, which provides
  more scalable behaviour for running registries on highly concurrent
  environments with thousands or millions of entries.

  ## Using in `:via`

  Once the registry is started with a given name, it can be used to
  register and access named processes using the
  `{:via, Registry, {registry, key}}` tuple:

      iex> {:ok, _} = Registry.start_link(:unique, Registry.ViaTest)
      iex> name = {:via, Registry, {Registry.ViaTest, "agent"}}
      iex> {:ok, _} = Agent.start_link(fn -> 0 end, name: name)
      iex> Agent.get(name, & &1)
      0
      iex> Agent.update(name, & &1 + 1)
      iex> Agent.get(name, & &1)
      1

  Typically the registry is started as part of a supervision tree though:

      supervisor(Registry, [:unique, Registry.ViaTest])

  Only registry with unique keys can be used in `:via`. If the name is
  already taken, the `start_link` function, such as `Agent.start_link/2`
  above, will return `{:error, {:already_started, current_pid}}`.

  ## Using as a dispatcher

  `Registry` has a dispatch mechanism that allows developers to implement
  custom dispatch logic triggered from the caller. For example:

      iex> {:ok, _} = Registry.start_link(:duplicate, Registry.DispatcherTest)
      iex> {:ok, _} = Registry.register(Registry.DispatcherTest, "hello", {IO, :inspect})
      iex> Registry.dispatch(Registry.DispatcherTest, "hello", fn entries ->
      ...>   for {pid, {module, function}} <- entries, do: apply(module, function, [pid])
      ...> end)
      # Prints #PID<...>
      :ok

  By calling `register/3` different processes can register under a given
  key and associate a specific `{module, function}` tuple under that key.
  Later on, an entity interested in dispatching events, may call `dispatch/3`,
  which will receive all entries under a given key, allowing the each
  `{module, function}` to be invoked.

  Keep in mind dispatching happens in the process that calls `dispatch/3`,
  registered processes are not involved in dispatching unless they are
  explicitly sent messages to. That's the example we will see next.

  ## Using as a PubSub

  Registries can also be used to implement a local, non-distributed,
  scalable PubSub by relying on the `dispatch/3` function, similar to
  the previous section, except we will send messages to each associated
  process, instead of invoking a given module-function.

  In this example, we will also set the number of partitions to the
  number of schedulers online, which will make the registry more performant
  on highly concurrent environments and allowing dispatching to happen
  in parallel:

      iex> {:ok, _} = Registry.start_link(:duplicate, Registry.PubSubTest,
      ...>                                partitions: System.schedulers_online)
      iex> {:ok, _} = Registry.register(Registry.PubSubTest, "hello", [])
      iex> Registry.dispatch(Registry.PubSubTest, "hello", fn entries ->
      ...>   for {pid, _} <- entries, do: send(pid, {:broadcast, "world"})
      ...> end)
      :ok

  The example above broadcasted the message `{:broadcast, "world"}` to all
  processes registered under the topic "hello".

  The third argument given to `register/3` is a value associated to the
  current process. While in the previous section we used it when dispatching,
  in this particular example we are not interested on it, so we have set it
  to an empty list. You could store a more meaningful value if necessary.

  ## Registrations

  Looking up, dispatching and registering are efficient and immediate at
  the cost of delayed unsubscription. For example, if a process crashes,
  its keys are automatically removed from the registry but the change may
  not propagate immediately. This means certain operations may return process
  that are already dead. When such may happen, it will be explicitly written
  in the function documentation.

  However, keep in mind those cases are typically not an issue. After all, a
  process referenced by a pid may crash at any time, including between getting
  the value from the registry and sending it a message. Many parts of the standard
  library are designed to cope with that, such as `Process.monitor/1` which will
  deliver the DOWN message immediately if the monitored process is already dead
  and `Kernel.send/2` which acts as a no-op for dead processes.
  """

  # TODO: Decide if it should be started as part of Elixir's supervision tree.

  @kind [:unique, :duplicate]

  @type registry :: atom
  @type kind :: :unique | :duplicate
  @type key :: term
  @type value :: term

  ## Via callbacks

  @doc false
  def whereis_name({registry, key}) do
    case whereis(registry, key) do
      {pid, _} -> pid
      :error -> :undefined
    end
  end

  @doc false
  def register_name({registry, key}, pid) when pid == self() do
    case register(registry, key, 0) do
      {:ok, _} -> :yes
      {:error, _} -> :no
    end
  end

  @doc false
  def send({registry, key}, msg) do
    case whereis(registry, key) do
      {pid, _} -> Kernel.send(pid, msg)
      :error -> :erlang.error(:badarg, [{registry, key}, msg])
    end
  end

  @doc false
  def unregister_name({registry, key}) do
    unregister(registry, key)
  end

  ## Registry API

  @doc """
  Starts the registry as a supervisor process.

  Manually it can be started as:

      Registry.start_link(:unique, MyApp.Registry)

  In your supervisor tree, you would write:

      supervisor(Registry, [:unique, MyApp.Registry])

  For intensive workloads, the registry may also be partitioned. If
  partioning is required a good default is to set the number of
  partitions to the number of schedulers available:

      Registry.start_link(:unique, MyApp.Registry, partitions: System.schedulers_online())

  or:

      supervisor(Registry, [:unique, MyApp.Registry, [partitions: System.schedulers_online()]])

  The registry uses one ETS table for metadata plus 2 ETS tables
  per partition.

  ## Options

  `options` is a keyword list with metadata that is stored in the
  registry and may be looked up using the `info/2` function. Only
  the following keys have meaning for the registry:

    * `:partitions` - the number of partitions in the registry. Defaults to 1.

  All other keys are stored as is.
  """
  @spec start_link(kind, registry, Keyword.t) :: {:ok, pid} | {:error, term}
  def start_link(kind, registry, options \\ []) when kind in @kind and is_atom(registry) do
    unless Keyword.keyword?(options) do
      raise ArgumentError, "expected Registry options to be a keyword list, got: #{inspect options}"
    end
    options = Keyword.put_new(options, :partitions, 1)
    Registry.Supervisor.start_link(kind, registry, options)
  end

  @doc """
  Invokes the callback with all entries under `key` in each partition
  for the given `registry`.

  The list of `entries` is a non-empty list of two-element tuples where
  the first element is the pid and the second element is the value
  associated to the pid.

  If the registry has unique keys, the callback function will be
  invoked only if it has a matching entry for key. The entries list
  will then contain a single element.

  If the registry has duplicate keys, the callback will be invoked
  per partition **concurrently**. The callback, however, won't be
  invoked if there are no entries for that particular partition.

  Keep in mind the `dispatch/3` function may return entries that have died
  but have not yet been removed from the table. If this can be an issue,
  consider explicitly checking if the process is alive in the entries
  table. Remember there are no guarantees though, after all, the
  process may also crash right after the `Process.alive?/1` check.

  See the module documentation for examples of using the `dispatch/3`
  function for building custom dispatching or a pubsub system.
  """
  @spec dispatch(registry, key, (entries :: [{pid, value}] -> term)) :: :ok
  def dispatch(registry, key, mfa_or_fun)
      when is_atom(registry) and is_function(mfa_or_fun, 1)
      when is_atom(registry) and tuple_size(mfa_or_fun) == 3 do
    case info!(registry) do
      {:unique, partitions} ->
        registry
        |> dispatch_lookup(:erlang.phash2(key, partitions), key)
        |> List.wrap()
        |> apply_non_empty_to_mfa_or_fun(mfa_or_fun)
      {:duplicate, 1} ->
        registry
        |> dispatch_lookup(0, key)
        |> apply_non_empty_to_mfa_or_fun(mfa_or_fun)
      {:duplicate, partitions} ->
        registry
        |> dispatch_task(key, mfa_or_fun, partitions)
        |> Enum.each(&Task.await(&1, :infinity))
    end
    :ok
  end

  defp dispatch_lookup(registry, partition, key) do
    try do
      :ets.lookup_element(key_ets!(registry, partition), key, 2)
    catch
      :error, :badarg -> []
    end
  end

  defp dispatch_task(_registry, _key, _mfa_or_fun, 0) do
    []
  end
  defp dispatch_task(registry, key, mfa_or_fun, partition) do
    partition = partition - 1
    task = Task.async(fn ->
      registry
      |> dispatch_lookup(partition, key)
      |> apply_non_empty_to_mfa_or_fun(mfa_or_fun)
      :ok
    end)
    [task | dispatch_task(registry, key, mfa_or_fun, partition)]
  end

  defp apply_non_empty_to_mfa_or_fun([], _mfa_or_fun) do
    :ok
  end
  defp apply_non_empty_to_mfa_or_fun(entries, {module, function, args}) do
    apply(module, function, [entries | args])
  end
  defp apply_non_empty_to_mfa_or_fun(entries, fun) do
    fun.(entries)
  end

  @doc """
  Finds the process for the given `key` in the unique `registry`.

  Returns `{pid, value}` if the registered `pid` is alive where
  `value` is the term associated to the pid on registration. Otherwise
  it returns `nil`.

  `ArgumentError` will be raised if a non-unique registry is given.

  ## Examples

  In the example below we register the current process and look it up
  both from itself and other processes.

      iex> Registry.start_link(:unique, Registry.WhereIsTest)
      iex> Registry.whereis(Registry.WhereIsTest, "hello")
      :error
      iex> {:ok, _} = Registry.register(Registry.WhereIsTest, "hello", :world)
      iex> Registry.whereis(Registry.WhereIsTest, "hello")
      {self(), :world}
      iex> Task.async(fn -> Registry.whereis(Registry.WhereIsTest, "hello") end) |> Task.await
      {self(), :world}

  """
  @spec whereis(registry, key) :: {pid, value} | :error
  def whereis(registry, key) when is_atom(registry) do
    case info!(registry) do
      {:unique, partitions} ->
        key_partition = :erlang.phash2(key, partitions)
        key_ets = key_ets!(registry, key_partition)
        case :ets.lookup(key_ets, key) do
          [{^key, {pid, _} = pair}] ->
            if Process.alive?(pid), do: pair, else: :error
          [] ->
            :error
        end
      {kind, _} ->
        raise ArgumentError, "Registry.whereis/2 not supported for #{kind} registries"
    end
  end

  @doc """
  Returns the known keys for the given `pid` in `registry` in no particular order.

  If the registry is unique, the keys are unique. Otherwise
  they may contain duplicates if the process was registered
  under the same key multiple times. The list will be empty
  if the process is dead or it has no keys in this registry.

  ## Examples

  Registering under a unique registry does not allow multiple entries:

      iex> Registry.start_link(:unique, Registry.UniqueKeysTest)
      iex> Registry.keys(Registry.UniqueKeysTest, self())
      []
      iex> {:ok, _} = Registry.register(Registry.UniqueKeysTest, "hello", :world)
      iex> Registry.register(Registry.UniqueKeysTest, "hello", :later)
      {:error, {:already_registered, self()}}
      iex> Registry.keys(Registry.UniqueKeysTest, self())
      ["hello"]

  Such is possible for duplicate registries though:

      iex> Registry.start_link(:duplicate, Registry.DuplicateKeysTest)
      iex> Registry.keys(Registry.DuplicateKeysTest, self())
      []
      iex> {:ok, _} = Registry.register(Registry.DuplicateKeysTest, "hello", :world)
      iex> {:ok, _} = Registry.register(Registry.DuplicateKeysTest, "hello", :world)
      iex> Registry.keys(Registry.DuplicateKeysTest, self())
      ["hello", "hello"]

  """
  @spec keys(registry, pid) :: [key]
  def keys(registry, pid) when is_atom(registry) and is_pid(pid) do
    {kind, partitions} = info!(registry)
    pid_partition = :erlang.phash2(pid, partitions)
    {_, pid_ets} = pid_ets!(registry, pid_partition)

    try do
      :ets.lookup_element(pid_ets, pid, 2)
    catch
      :error, :badarg -> []
    else
      keys ->
        cond do
          not Process.alive?(pid) -> []
          kind == :unique -> Enum.uniq(keys)
          true -> keys
        end
    end
  end

  @doc """
  Unregisters all entries for the given `key` associated to the current
  process in `registry`.

  Always returns `:ok` and automatically unlinks the current process from
  the owner if there are no more keys associated to the current process.

  ## Examples

  It unregister all entries for `key` for unique registries:

      iex> Registry.start_link(:unique, Registry.UniqueUnregisterTest)
      iex> Registry.register(Registry.UniqueUnregisterTest, "hello", :world)
      iex> Registry.keys(Registry.UniqueUnregisterTest, self())
      ["hello"]
      iex> Registry.unregister(Registry.UniqueUnregisterTest, "hello")
      :ok
      iex> Registry.keys(Registry.UniqueUnregisterTest, self())
      []

  As well as duplicate registries:

      iex> Registry.start_link(:duplicate, Registry.DuplicateUnregisterTest)
      iex> Registry.register(Registry.DuplicateUnregisterTest, "hello", :world)
      iex> Registry.register(Registry.DuplicateUnregisterTest, "hello", :world)
      iex> Registry.keys(Registry.DuplicateUnregisterTest, self())
      ["hello", "hello"]
      iex> Registry.unregister(Registry.DuplicateUnregisterTest, "hello")
      :ok
      iex> Registry.keys(Registry.DuplicateUnregisterTest, self())
      []

  """
  @spec unregister(registry, key) :: :ok
  def unregister(registry, key) when is_atom(registry) do
    self = self()
    {kind, partitions} = info!(registry)
    {key_partition, pid_partition} =
      Registry.Partition.partitions(kind, key, self, partitions)
    key_ets = key_ets!(registry, key_partition)
    {pid_server, pid_ets} = pid_ets!(registry, pid_partition)

    # Remove first from the key_ets because in case of crashes
    # the pid_ets will still be able to clean up. The last step is
    # to clean if we have no more entries.
    true = :ets.match_delete(key_ets, {key, {self, :_}})
    true = :ets.delete_object(pid_ets, {self, key, key_ets})

    case :ets.select_count(pid_ets, [{{self, :_, :_}, [], [true]}]) do
      0 -> Process.unlink(pid_server)
      _ -> :ok
    end
    :ok
  end

  @doc """
  Registers the current process under the given `key` in `registry`.

  A value to be associated with this registration must also be given.
  This value will be retrieved whenever dispatching or doing a key
  lookup.

  This function returns `{:ok, owner}` or `{:error, reason}`.
  The `owner` is the pid of the registry partition responsible for
  the pid. The owner is automatically linked to the caller.

  If the registry has unique keys, it will return `{:ok, owner}` unless
  the key is already associated to a pid, in which case it returns
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
  @spec register(registry, key, value) :: {:ok, pid} | {:error, {:already_registered, pid}}
  def register(registry, key, value) when is_atom(registry) do
    self = self()
    {kind, partitions} = info!(registry)
    {key_partition, pid_partition} =
      Registry.Partition.partitions(kind, key, self, partitions)
    key_ets = key_ets!(registry, key_partition)
    {pid_server, pid_ets} = pid_ets!(registry, pid_partition)

    # Notice we write first to the pid ets table because it will
    # always be able to do the clean up. If we register first to the
    # key one and the process crashes, the key will stay there forever.
    Process.link(pid_server)
    true = :ets.insert(pid_ets, {self, key, key_ets})
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
      {:custom_key, "custom_value"}
      iex> Registry.info(Registry.InfoDocTest, :unknown_key)
      :error

  """
  @spec info(registry, info_key :: atom) :: {info_key :: atom, info_value :: term} | :error
  def info(registry, key) when is_atom(registry) and is_atom(key) do
    try do
      :ets.lookup(registry, key)
    catch
      :error, :badarg ->
        raise ArgumentError, "unknown registry: #{inspect registry}"
    else
      [{^key, _} = pair] -> pair
      _ -> :error
    end
  end

  ## Helpers

  defp info!(registry) do
    try do
      :ets.lookup(registry, :__info__)
    catch
      :error, :badarg ->
        raise ArgumentError, "unknown registry: #{inspect registry}"
    else
      [{:__info__, kind, partitions}] -> {kind, partitions}
    end
  end

  defp key_ets!(registry, partition) do
    :ets.lookup_element(registry, partition, 2)
  end

  defp pid_ets!(registry, partition) do
    :ets.lookup_element(registry, partition, 3)
  end
end

defmodule Registry.Supervisor do
  @moduledoc false
  use Supervisor

  def start_link(kind, registry, options) do
    Supervisor.start_link(__MODULE__, {kind, registry, options}, name: registry)
  end

  def init({kind, registry, options}) do
    partitions = Keyword.get(options, :partitions, 1)
    ^registry = :ets.new(registry, [:set, :public, :named_table, read_concurrency: true])
    true = :ets.insert(registry, options)
    true = :ets.insert(registry, {:__info__, kind, partitions})

    children =
      for i <- 0..partitions-1 do
        key_partition = Registry.Partition.key_name(registry, i)
        pid_partition = Registry.Partition.pid_name(registry, i)
        arg = {kind, registry, i, key_partition, pid_partition}
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
  def key_name(registry, partition) do
    Module.concat([registry, "KeyPartition" <> Integer.to_string(partition)])
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
  def start_link(registry, arg) do
    GenServer.start_link(__MODULE__, arg, name: registry)
  end

  ## Callbacks

  def init({kind, registry, i, key_partition, pid_partition}) do
    Process.flag(:trap_exit, true)
    key_ets = init_key_ets(kind, key_partition)
    pid_ets = init_pid_ets(kind, pid_partition)
    true = :ets.insert(registry, {i, key_ets, {self(), pid_ets}})
    {:ok, pid_ets}
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

  def handle_call(:sync, _, state) do
    {:reply, :ok, state}
  end

  def handle_info({:EXIT, pid, _reason}, ets) do
    entries = :ets.take(ets, pid)
    for {_pid, key, key_ets} <- entries do
      try do
        :ets.match_delete(key_ets, {key, {pid, :_}})
      catch
        :error, :badarg -> :badarg
      end
    end
    {:noreply, ets}
  end
  def handle_info(msg, state) do
    super(msg, state)
  end
end
